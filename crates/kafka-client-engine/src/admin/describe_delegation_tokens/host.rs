//! Bounded ownership of accepted token-description machines and API-41 calls.

mod admission;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    DescribeDelegationTokensEffect, DescribeDelegationTokensInput, DescribeDelegationTokensMachine,
    DescribeDelegationTokensPlan, DescribeDelegationTokensTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminDescribeDelegationTokensPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{DescribeDelegationTokensCall, DescribeDelegationTokensRawTerminal},
    protocol::admin::describe_delegation_tokens::PreparedDescribeDelegationTokensRequest,
};

use super::{DescribeDelegationTokensHostError, DescribeDelegationTokensObserver};

pub(crate) const DESCRIBE_DELEGATION_TOKENS_CAPACITY: usize = 16;
pub(crate) const DESCRIBE_DELEGATION_TOKENS_OPERATION_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const DESCRIBE_DELEGATION_TOKENS_RETAINED_BYTES: usize =
    DESCRIBE_DELEGATION_TOKENS_CAPACITY * DESCRIBE_DELEGATION_TOKENS_OPERATION_BYTES;
pub(crate) struct DescribeDelegationTokensAdmission {
    pub(crate) observer: DescribeDelegationTokensObserver,
    pub(crate) fault: Option<DescribeDelegationTokensHostError>,
}

/// One plan and prepared API-key 41 owner ready for `AnyBroker` handoff.
pub(crate) struct DescribeDelegationTokensSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    plan: DescribeDelegationTokensPlan,
    prepared_request: PreparedDescribeDelegationTokensRequest,
}

impl DescribeDelegationTokensSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        DescribeDelegationTokensPlan,
        PreparedDescribeDelegationTokensRequest,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.prepared_request,
        )
    }
}

pub(crate) enum DescribeDelegationTokensTurn {
    Idle,
    Progress,
    Submit(DescribeDelegationTokensSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DescribeDelegationTokensHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct DescribeDelegationTokensOperation {
    operation_id: OperationId,
    machine: DescribeDelegationTokensMachine,
    correlation_plan: Option<DescribeDelegationTokensPlan>,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<DescribeDelegationTokensSubmission>,
    handoff: DescribeDelegationTokensHandoff,
    call: Option<DescribeDelegationTokensCall>,
    recovered_call: Option<crate::driver::RecoveredDescribeDelegationTokensCall>,
    raw_terminal: Option<DescribeDelegationTokensRawTerminal>,
    terminal: Option<DescribeDelegationTokensTerminal>,
}

impl DescribeDelegationTokensOperation {
    fn take_submission_for_handoff(
        &mut self,
    ) -> Result<DescribeDelegationTokensSubmission, DescribeDelegationTokensHostError> {
        let submission = self
            .submission
            .take()
            .ok_or(DescribeDelegationTokensHostError::MissingSubmission)?;
        self.handoff = DescribeDelegationTokensHandoff::HandedOff;
        Ok(submission)
    }

    fn accept_call(
        &mut self,
        call: DescribeDelegationTokensCall,
    ) -> Result<(), DescribeDelegationTokensHostError> {
        if self.handoff != DescribeDelegationTokensHandoff::HandedOff || self.call.is_some() {
            return Err(DescribeDelegationTokensHostError::InvalidHandoff);
        }
        self.call = Some(call);
        Ok(())
    }

    fn apply_input(
        &mut self,
        input: DescribeDelegationTokensInput,
    ) -> Result<bool, DescribeDelegationTokensHostError> {
        let accepted = matches!(&input, DescribeDelegationTokensInput::DriverAccepted);
        if accepted && self.handoff != DescribeDelegationTokensHandoff::HandedOff {
            return Err(DescribeDelegationTokensHostError::InvalidHandoff);
        }
        let transition = self.machine.apply(input)?;
        if accepted {
            self.handoff = DescribeDelegationTokensHandoff::Submitted;
        }
        if let Some(DescribeDelegationTokensEffect::Complete { terminal, .. }) =
            transition.into_effect()
        {
            self.terminal = Some(terminal);
            return Ok(true);
        }
        Ok(false)
    }
}

pub(crate) struct DescribeDelegationTokensHost {
    operations: Vec<DescribeDelegationTokensOperation>,
    completions: CompletionRegistry<
        DescribeDelegationTokensTerminal,
        AdminDescribeDelegationTokensPublisher,
    >,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<DescribeDelegationTokensHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl DescribeDelegationTokensHost {
    pub(crate) fn new(publisher: AdminDescribeDelegationTokensPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DESCRIBE_DELEGATION_TOKENS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                DESCRIBE_DELEGATION_TOKENS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DESCRIBE_DELEGATION_TOKENS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<DescribeDelegationTokensTurn, DescribeDelegationTokensHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(DescribeDelegationTokensTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DescribeDelegationTokensTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, DescribeDelegationTokensInput::DeadlineElapsed)?;
            return Ok(DescribeDelegationTokensTurn::Progress);
        }
        let submission = self.operations[index].take_submission_for_handoff()?;
        Ok(DescribeDelegationTokensTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: DescribeDelegationTokensCall,
    ) -> Result<(), DescribeDelegationTokensHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeDelegationTokensHostError::UnknownOperation)?;
        self.operations[index].accept_call(call)?;
        self.apply(operation_id, DescribeDelegationTokensInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), DescribeDelegationTokensHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeDelegationTokensHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeDelegationTokensHandoff::HandedOff {
            return Err(DescribeDelegationTokensHostError::InvalidHandoff);
        }
        self.apply(operation_id, DescribeDelegationTokensInput::DriverRejected)
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
        input: DescribeDelegationTokensInput,
    ) -> Result<(), DescribeDelegationTokensHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeDelegationTokensHostError::UnknownOperation)?;
        if self.operations[index].apply_input(input)? {
            self.publish_terminal(index)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }
}
