//! Bounded ownership of accepted token-expiration machines and API-40 calls.

mod admission;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    ExpireDelegationTokenEffect, ExpireDelegationTokenInput, ExpireDelegationTokenMachine,
    ExpireDelegationTokenPlan, ExpireDelegationTokenTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminExpireDelegationTokenPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{
        ExpireDelegationTokenCall, ExpireDelegationTokenRawTerminal,
        RecoveredExpireDelegationTokenCall,
    },
    protocol::admin::expire_delegation_token::PreparedExpireDelegationTokenRequest,
};

use super::{ExpireDelegationTokenHostError, ExpireDelegationTokenObserver};

pub(crate) const EXPIRE_DELEGATION_TOKEN_CAPACITY: usize = 16;
pub(crate) const EXPIRE_DELEGATION_TOKEN_OPERATION_BYTES: usize = 1024 * 1024;
pub(crate) const EXPIRE_DELEGATION_TOKEN_RETAINED_BYTES: usize =
    EXPIRE_DELEGATION_TOKEN_CAPACITY * EXPIRE_DELEGATION_TOKEN_OPERATION_BYTES;

pub(crate) struct ExpireDelegationTokenAdmission {
    pub(crate) observer: ExpireDelegationTokenObserver,
    pub(crate) fault: Option<ExpireDelegationTokenHostError>,
}

/// One plan and prepared API-key 40 owner ready for AnyBroker handoff.
pub(crate) struct ExpireDelegationTokenSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    plan: ExpireDelegationTokenPlan,
    prepared_request: PreparedExpireDelegationTokenRequest,
}

impl ExpireDelegationTokenSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        ExpireDelegationTokenPlan,
        PreparedExpireDelegationTokenRequest,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.prepared_request,
        )
    }
}

pub(crate) enum ExpireDelegationTokenTurn {
    Idle,
    Progress,
    Submit(ExpireDelegationTokenSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExpireDelegationTokenHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct ExpireDelegationTokenOperation {
    operation_id: OperationId,
    machine: ExpireDelegationTokenMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<ExpireDelegationTokenSubmission>,
    handoff: ExpireDelegationTokenHandoff,
    call: Option<ExpireDelegationTokenCall>,
    recovered_call: Option<RecoveredExpireDelegationTokenCall>,
    raw_terminal: Option<ExpireDelegationTokenRawTerminal>,
    terminal: Option<ExpireDelegationTokenTerminal>,
}

pub(crate) struct ExpireDelegationTokenHost {
    operations: Vec<ExpireDelegationTokenOperation>,
    completions:
        CompletionRegistry<ExpireDelegationTokenTerminal, AdminExpireDelegationTokenPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<ExpireDelegationTokenHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl ExpireDelegationTokenHost {
    pub(crate) fn new(publisher: AdminExpireDelegationTokenPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(EXPIRE_DELEGATION_TOKEN_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                EXPIRE_DELEGATION_TOKEN_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(EXPIRE_DELEGATION_TOKEN_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<ExpireDelegationTokenTurn, ExpireDelegationTokenHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(ExpireDelegationTokenTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(ExpireDelegationTokenTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, ExpireDelegationTokenInput::DeadlineElapsed)?;
            return Ok(ExpireDelegationTokenTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(ExpireDelegationTokenHostError::MissingSubmission)?;
        self.operations[index].handoff = ExpireDelegationTokenHandoff::HandedOff;
        Ok(ExpireDelegationTokenTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: ExpireDelegationTokenCall,
    ) -> Result<(), ExpireDelegationTokenHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ExpireDelegationTokenHostError::UnknownOperation)?;
        if self.operations[index].handoff != ExpireDelegationTokenHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
        {
            return Err(ExpireDelegationTokenHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, ExpireDelegationTokenInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), ExpireDelegationTokenHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ExpireDelegationTokenHostError::UnknownOperation)?;
        if self.operations[index].handoff != ExpireDelegationTokenHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(ExpireDelegationTokenHostError::InvalidHandoff);
        }
        self.apply(operation_id, ExpireDelegationTokenInput::DriverRejected)
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
        input: ExpireDelegationTokenInput,
    ) -> Result<(), ExpireDelegationTokenHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ExpireDelegationTokenHostError::UnknownOperation)?;
        let accepted = matches!(&input, ExpireDelegationTokenInput::DriverAccepted);
        if accepted && self.operations[index].handoff != ExpireDelegationTokenHandoff::HandedOff {
            return Err(ExpireDelegationTokenHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = ExpireDelegationTokenHandoff::Submitted;
        }
        if let Some(ExpireDelegationTokenEffect::Complete { terminal, .. }) =
            transition.into_effect()
        {
            self.operations[index].terminal = Some(terminal);
            self.publish_terminal(index)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }
}
