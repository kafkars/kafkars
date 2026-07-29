//! Bounded ownership of accepted token-renewal machines and API-39 calls.

mod admission;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    Moment, OperationId, RenewDelegationTokenEffect, RenewDelegationTokenInput,
    RenewDelegationTokenMachine, RenewDelegationTokenPlan, RenewDelegationTokenTerminal,
};

use crate::{
    admin::AdminRenewDelegationTokenPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{RenewDelegationTokenCall, RenewDelegationTokenRawTerminal},
    protocol::admin::renew_delegation_token::PreparedRenewDelegationTokenRequest,
};

use super::{RenewDelegationTokenHostError, RenewDelegationTokenObserver};

pub(crate) const RENEW_DELEGATION_TOKEN_CAPACITY: usize = 16;
pub(crate) const RENEW_DELEGATION_TOKEN_OPERATION_BYTES: usize = 1024 * 1024;
pub(crate) const RENEW_DELEGATION_TOKEN_RETAINED_BYTES: usize =
    RENEW_DELEGATION_TOKEN_CAPACITY * RENEW_DELEGATION_TOKEN_OPERATION_BYTES;

pub(crate) struct RenewDelegationTokenAdmission {
    pub(crate) observer: RenewDelegationTokenObserver,
    pub(crate) fault: Option<RenewDelegationTokenHostError>,
}

/// One plan and prepared API-key 39 owner ready for AnyBroker handoff.
pub(crate) struct RenewDelegationTokenSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    plan: RenewDelegationTokenPlan,
    prepared_request: PreparedRenewDelegationTokenRequest,
}

impl RenewDelegationTokenSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        RenewDelegationTokenPlan,
        PreparedRenewDelegationTokenRequest,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.prepared_request,
        )
    }
}

pub(crate) enum RenewDelegationTokenTurn {
    Idle,
    Progress,
    Submit(RenewDelegationTokenSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RenewDelegationTokenHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct RenewDelegationTokenOperation {
    operation_id: OperationId,
    machine: RenewDelegationTokenMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<RenewDelegationTokenSubmission>,
    handoff: RenewDelegationTokenHandoff,
    call: Option<RenewDelegationTokenCall>,
    raw_terminal: Option<RenewDelegationTokenRawTerminal>,
    terminal: Option<RenewDelegationTokenTerminal>,
}

pub(crate) struct RenewDelegationTokenHost {
    operations: Vec<RenewDelegationTokenOperation>,
    completions:
        CompletionRegistry<RenewDelegationTokenTerminal, AdminRenewDelegationTokenPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<RenewDelegationTokenHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl RenewDelegationTokenHost {
    pub(crate) fn new(publisher: AdminRenewDelegationTokenPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(RENEW_DELEGATION_TOKEN_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                RENEW_DELEGATION_TOKEN_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(RENEW_DELEGATION_TOKEN_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<RenewDelegationTokenTurn, RenewDelegationTokenHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(RenewDelegationTokenTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(RenewDelegationTokenTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, RenewDelegationTokenInput::DeadlineElapsed)?;
            return Ok(RenewDelegationTokenTurn::Progress);
        }
        let submission = take_submission_for_handoff(&mut self.operations[index])?;
        Ok(RenewDelegationTokenTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: RenewDelegationTokenCall,
    ) -> Result<(), RenewDelegationTokenHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(RenewDelegationTokenHostError::UnknownOperation)?;
        bind_call(&mut self.operations[index], call)?;
        self.apply(operation_id, RenewDelegationTokenInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), RenewDelegationTokenHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(RenewDelegationTokenHostError::UnknownOperation)?;
        if self.operations[index].handoff != RenewDelegationTokenHandoff::HandedOff {
            return Err(RenewDelegationTokenHostError::InvalidHandoff);
        }
        self.apply(operation_id, RenewDelegationTokenInput::DriverRejected)
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
        input: RenewDelegationTokenInput,
    ) -> Result<(), RenewDelegationTokenHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(RenewDelegationTokenHostError::UnknownOperation)?;
        if apply_input(&mut self.operations[index], input)? {
            self.publish_terminal(index)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }
}

fn take_submission_for_handoff(
    operation: &mut RenewDelegationTokenOperation,
) -> Result<RenewDelegationTokenSubmission, RenewDelegationTokenHostError> {
    let submission = operation
        .submission
        .take()
        .ok_or(RenewDelegationTokenHostError::MissingSubmission)?;
    operation.handoff = RenewDelegationTokenHandoff::HandedOff;
    Ok(submission)
}

fn bind_call(
    operation: &mut RenewDelegationTokenOperation,
    call: RenewDelegationTokenCall,
) -> Result<(), RenewDelegationTokenHostError> {
    if operation.handoff != RenewDelegationTokenHandoff::HandedOff || operation.call.is_some() {
        return Err(RenewDelegationTokenHostError::InvalidHandoff);
    }
    operation.call = Some(call);
    Ok(())
}

fn apply_input(
    operation: &mut RenewDelegationTokenOperation,
    input: RenewDelegationTokenInput,
) -> Result<bool, RenewDelegationTokenHostError> {
    let accepted = matches!(&input, RenewDelegationTokenInput::DriverAccepted);
    if accepted && operation.handoff != RenewDelegationTokenHandoff::HandedOff {
        return Err(RenewDelegationTokenHostError::InvalidHandoff);
    }
    let machine = &mut operation.machine;
    let transition = machine.apply(input)?;
    if accepted {
        operation.handoff = RenewDelegationTokenHandoff::Submitted;
    }
    if let Some(RenewDelegationTokenEffect::Complete { terminal, .. }) = transition.into_effect() {
        operation.terminal = Some(terminal);
        return Ok(true);
    }
    Ok(false)
}
