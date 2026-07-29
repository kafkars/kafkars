//! Bounded ownership of voter-removal machines and concrete calls.

mod admission;
mod model;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    Moment, OperationId, RemoveRaftVoterEffect, RemoveRaftVoterInput, RemoveRaftVoterMachine,
    RemoveRaftVoterTerminal,
};

use crate::{
    admin::AdminRemoveRaftVoterPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{
        DriverOwner, RecoveredRemoveRaftVoterCall, RemoveRaftVoterCall, RemoveRaftVoterRawTerminal,
    },
};

use super::{RemoveRaftVoterHostError, RemoveRaftVoterObserver};

use model::RemoveRaftVoterHandoff;
pub(crate) use model::{RemoveRaftVoterSubmission, RemoveRaftVoterTurn};

pub(crate) const REMOVE_RAFT_VOTER_CAPACITY: usize = 16;
/// Maximum normalized retained result bytes charged per operation.
pub(crate) const REMOVE_RAFT_VOTER_RESULT_BYTES: usize = 4 * 1024;
/// Aggregate bytes retained by active and published voter removals.
pub(crate) const REMOVE_RAFT_VOTER_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct RemoveRaftVoterAdmission {
    pub(crate) observer: RemoveRaftVoterObserver,
    pub(crate) fault: Option<RemoveRaftVoterHostError>,
}

struct RemoveRaftVoterOperation {
    operation_id: OperationId,
    machine: RemoveRaftVoterMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<RemoveRaftVoterSubmission>,
    handoff: RemoveRaftVoterHandoff,
    call: Option<RemoveRaftVoterCall>,
    recovered_call: Option<RecoveredRemoveRaftVoterCall>,
    raw_terminal: Option<RemoveRaftVoterRawTerminal>,
    terminal: Option<RemoveRaftVoterTerminal>,
}

pub(crate) struct RemoveRaftVoterHost {
    operations: Vec<RemoveRaftVoterOperation>,
    completions: CompletionRegistry<RemoveRaftVoterTerminal, AdminRemoveRaftVoterPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<RemoveRaftVoterHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl RemoveRaftVoterHost {
    pub(crate) fn new(publisher: AdminRemoveRaftVoterPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(REMOVE_RAFT_VOTER_CAPACITY),
            completions: CompletionRegistry::with_publisher(REMOVE_RAFT_VOTER_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(REMOVE_RAFT_VOTER_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
        driver: Option<&DriverOwner>,
    ) -> Result<RemoveRaftVoterTurn, RemoveRaftVoterHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call(driver)? {
            return Ok(RemoveRaftVoterTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(RemoveRaftVoterTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, RemoveRaftVoterInput::DeadlineElapsed)?;
            return Ok(RemoveRaftVoterTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(RemoveRaftVoterHostError::MissingSubmission)?;
        self.operations[index].handoff = RemoveRaftVoterHandoff::HandedOff;
        Ok(RemoveRaftVoterTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: RemoveRaftVoterCall,
    ) -> Result<(), RemoveRaftVoterHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(RemoveRaftVoterHostError::UnknownOperation)?;
        if self.operations[index].handoff != RemoveRaftVoterHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
        {
            return Err(RemoveRaftVoterHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, RemoveRaftVoterInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), RemoveRaftVoterHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(RemoveRaftVoterHostError::UnknownOperation)?;
        if self.operations[index].handoff != RemoveRaftVoterHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(RemoveRaftVoterHostError::InvalidHandoff);
        }
        self.apply(operation_id, RemoveRaftVoterInput::DriverRejected)
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
        input: RemoveRaftVoterInput,
    ) -> Result<(), RemoveRaftVoterHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(RemoveRaftVoterHostError::UnknownOperation)?;
        let accepted = matches!(&input, RemoveRaftVoterInput::DriverAccepted);
        if accepted && self.operations[index].handoff != RemoveRaftVoterHandoff::HandedOff {
            return Err(RemoveRaftVoterHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = RemoveRaftVoterHandoff::Submitted;
        }
        if let Some(RemoveRaftVoterEffect::Complete { terminal, .. }) = transition.into_effect() {
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
