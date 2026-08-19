//! Bounded ownership of accepted token-creation machines and API-38 calls.

mod admission;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    CreateDelegationTokenEffect, CreateDelegationTokenInput, CreateDelegationTokenMachine,
    CreateDelegationTokenPlan, CreateDelegationTokenTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminCreateDelegationTokenPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{
        CreateDelegationTokenCall, CreateDelegationTokenRawTerminal,
        RecoveredCreateDelegationTokenCall,
    },
    protocol::admin::create_delegation_token::PreparedCreateDelegationTokenRequest,
};

use super::{CreateDelegationTokenHostError, CreateDelegationTokenObserver};

pub(crate) const CREATE_DELEGATION_TOKEN_CAPACITY: usize = 16;
pub(crate) const CREATE_DELEGATION_TOKEN_OPERATION_BYTES: usize = 1024 * 1024;
pub(crate) const CREATE_DELEGATION_TOKEN_RETAINED_BYTES: usize =
    CREATE_DELEGATION_TOKEN_CAPACITY * CREATE_DELEGATION_TOKEN_OPERATION_BYTES;

pub(crate) struct CreateDelegationTokenAdmission {
    pub(crate) observer: CreateDelegationTokenObserver,
    pub(crate) fault: Option<CreateDelegationTokenHostError>,
}

/// One plan and prepared API-key 38 owner ready for `AnyBroker` handoff.
pub(crate) struct CreateDelegationTokenSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    plan: CreateDelegationTokenPlan,
    prepared_request: PreparedCreateDelegationTokenRequest,
}

impl CreateDelegationTokenSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        CreateDelegationTokenPlan,
        PreparedCreateDelegationTokenRequest,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.prepared_request,
        )
    }
}

#[allow(
    clippy::large_enum_variant,
    reason = "the bounded turn transfers prepared ownership without a post-admission box allocation"
)]
pub(crate) enum CreateDelegationTokenTurn {
    Idle,
    Progress,
    Submit(CreateDelegationTokenSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreateDelegationTokenHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct CreateDelegationTokenOperation {
    operation_id: OperationId,
    machine: CreateDelegationTokenMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<CreateDelegationTokenSubmission>,
    handoff: CreateDelegationTokenHandoff,
    call: Option<CreateDelegationTokenCall>,
    recovered_call: Option<RecoveredCreateDelegationTokenCall>,
    raw_terminal: Option<CreateDelegationTokenRawTerminal>,
    terminal: Option<CreateDelegationTokenTerminal>,
}

impl CreateDelegationTokenOperation {
    fn take_submission_for_handoff(
        &mut self,
    ) -> Result<CreateDelegationTokenSubmission, CreateDelegationTokenHostError> {
        let submission = self
            .submission
            .take()
            .ok_or(CreateDelegationTokenHostError::MissingSubmission)?;
        self.handoff = CreateDelegationTokenHandoff::HandedOff;
        Ok(submission)
    }

    fn accept_call(
        &mut self,
        call: CreateDelegationTokenCall,
    ) -> Result<(), CreateDelegationTokenHostError> {
        if self.handoff != CreateDelegationTokenHandoff::HandedOff
            || self.call.is_some()
            || self.recovered_call.is_some()
        {
            return Err(CreateDelegationTokenHostError::InvalidHandoff);
        }
        self.call = Some(call);
        Ok(())
    }

    fn apply_input(
        &mut self,
        input: CreateDelegationTokenInput,
    ) -> Result<bool, CreateDelegationTokenHostError> {
        let accepted = matches!(&input, CreateDelegationTokenInput::DriverAccepted);
        if accepted && self.handoff != CreateDelegationTokenHandoff::HandedOff {
            return Err(CreateDelegationTokenHostError::InvalidHandoff);
        }
        let transition = self.machine.apply(input)?;
        if accepted {
            self.handoff = CreateDelegationTokenHandoff::Submitted;
        }
        if let Some(CreateDelegationTokenEffect::Complete { terminal, .. }) =
            transition.into_effect()
        {
            self.terminal = Some(terminal);
            return Ok(true);
        }
        Ok(false)
    }
}

pub(crate) struct CreateDelegationTokenHost {
    operations: Vec<CreateDelegationTokenOperation>,
    completions:
        CompletionRegistry<CreateDelegationTokenTerminal, AdminCreateDelegationTokenPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<CreateDelegationTokenHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl CreateDelegationTokenHost {
    pub(crate) fn new(publisher: AdminCreateDelegationTokenPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(CREATE_DELEGATION_TOKEN_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                CREATE_DELEGATION_TOKEN_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(CREATE_DELEGATION_TOKEN_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<CreateDelegationTokenTurn, CreateDelegationTokenHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(CreateDelegationTokenTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(CreateDelegationTokenTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, CreateDelegationTokenInput::DeadlineElapsed)?;
            return Ok(CreateDelegationTokenTurn::Progress);
        }
        let submission = self.operations[index].take_submission_for_handoff()?;
        Ok(CreateDelegationTokenTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: CreateDelegationTokenCall,
    ) -> Result<(), CreateDelegationTokenHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(CreateDelegationTokenHostError::UnknownOperation)?;
        self.operations[index].accept_call(call)?;
        self.apply(operation_id, CreateDelegationTokenInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), CreateDelegationTokenHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(CreateDelegationTokenHostError::UnknownOperation)?;
        if self.operations[index].handoff != CreateDelegationTokenHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
        {
            return Err(CreateDelegationTokenHostError::InvalidHandoff);
        }
        self.apply(operation_id, CreateDelegationTokenInput::DriverRejected)
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
        input: CreateDelegationTokenInput,
    ) -> Result<(), CreateDelegationTokenHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(CreateDelegationTokenHostError::UnknownOperation)?;
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
