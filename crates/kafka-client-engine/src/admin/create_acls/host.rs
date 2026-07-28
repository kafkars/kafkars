//! Bounded ownership of accepted Admin `CreateAcls` machines and calls.

mod admission;
mod error;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    CreateAclResult, CreateAclsEffect, CreateAclsInput, CreateAclsMachine, CreateAclsPlan,
    CreateAclsTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminCreateAclsPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{CreateAclsCall, CreateAclsRawTerminal, RecoveredCreateAclsCall},
};

pub(crate) use error::CreateAclsHostError;

use super::{CreateAclOutcome, CreateAclsObserver, CreateAclsOutcome};

pub(crate) const CREATE_ACLS_CAPACITY: usize = 16;
pub(crate) const CREATE_ACLS_RETAINED_BYTES: usize = 8 * 1024 * 1024;

pub(crate) struct CreateAclsAdmission {
    pub(crate) observer: CreateAclsObserver,
    pub(crate) fault: Option<CreateAclsHostError>,
}

/// One caller-ordered creation plan ready for driver admission.
pub(crate) struct CreateAclsSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    plan: CreateAclsPlan,
    request_limit: usize,
    result_limit: usize,
}

impl CreateAclsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (OperationId, OperationDeadline, CreateAclsPlan, usize, usize) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.request_limit,
            self.result_limit,
        )
    }
}

pub(crate) enum CreateAclsTurn {
    Idle,
    Progress,
    Submit(CreateAclsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreateAclsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct CreateAclsOperation {
    operation_id: OperationId,
    machine: CreateAclsMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    request_limit: usize,
    result_limit: usize,
    remaining_result_bytes: usize,
    prepared_results: Option<Vec<CreateAclResult>>,
    prepared_outcomes: Option<Vec<CreateAclOutcome>>,
    submission: Option<CreateAclsSubmission>,
    handoff: CreateAclsHandoff,
    call: Option<CreateAclsCall>,
    // Driver-shutdown proof remains live until core accepts the terminal fact.
    recovered_call: Option<RecoveredCreateAclsCall>,
    raw_terminal: Option<CreateAclsRawTerminal>,
    terminal: Option<CreateAclsTerminal>,
    outcome: Option<CreateAclsOutcome>,
}

pub(crate) struct CreateAclsHost {
    operations: Vec<CreateAclsOperation>,
    completions: CompletionRegistry<CreateAclsOutcome, AdminCreateAclsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<CreateAclsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl CreateAclsHost {
    pub(crate) fn new(publisher: AdminCreateAclsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(CREATE_ACLS_CAPACITY),
            completions: CompletionRegistry::with_publisher(CREATE_ACLS_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(CREATE_ACLS_CAPACITY),
        }
    }

    pub(crate) fn turn(&mut self, now: Moment) -> Result<CreateAclsTurn, CreateAclsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(CreateAclsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(CreateAclsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, CreateAclsInput::DeadlineElapsed)?;
            return Ok(CreateAclsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(CreateAclsHostError::MissingSubmission)?;
        self.operations[index].handoff = CreateAclsHandoff::HandedOff;
        Ok(CreateAclsTurn::Submit(submission))
    }

    pub(crate) fn close_admission(&mut self) {
        self.accepting = false;
    }

    pub(crate) fn unsettled(&self) -> usize {
        self.operations.len()
    }

    pub(crate) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.operations
            .iter()
            .filter(|operation| operation.submission.is_some())
            .map(|operation| operation.deadline.core())
            .min()
    }

    fn operation_index(&self, operation_id: OperationId) -> Option<usize> {
        self.operations
            .iter()
            .position(|operation| operation.operation_id == operation_id)
    }

    fn apply(
        &mut self,
        operation_id: OperationId,
        input: CreateAclsInput,
    ) -> Result<(), CreateAclsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(CreateAclsHostError::UnknownOperation)?;
        let accepted = matches!(&input, CreateAclsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != CreateAclsHandoff::HandedOff {
            return Err(CreateAclsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = CreateAclsHandoff::Submitted;
        }
        if let Some(CreateAclsEffect::Complete { terminal, .. }) = transition.into_effect() {
            self.operations[index].terminal = Some(terminal);
            self.publish_terminal(index)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }

    #[cfg(test)]
    pub(super) fn prepared_result_capacity_for_test(&self) -> Option<usize> {
        self.operations
            .first()
            .and_then(|operation| operation.prepared_results.as_ref())
            .map(Vec::capacity)
    }

    #[cfg(test)]
    pub(super) fn prepared_outcome_capacity_for_test(&self) -> Option<usize> {
        self.operations
            .first()
            .and_then(|operation| operation.prepared_outcomes.as_ref())
            .map(Vec::capacity)
    }
}
