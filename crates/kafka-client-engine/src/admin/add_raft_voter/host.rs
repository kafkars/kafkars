//! Bounded ownership of accepted voter-addition machines and API80 calls.

mod admission;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    AddRaftVoterEffect, AddRaftVoterInput, AddRaftVoterMachine, AddRaftVoterPlan,
    AddRaftVoterTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminAddRaftVoterPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{AddRaftVoterCall, AddRaftVoterRawTerminal, RecoveredAddRaftVoterCall},
};

use super::{AddRaftVoterHostError, AddRaftVoterObserver};

pub(crate) const ADD_RAFT_VOTER_CAPACITY: usize = 16;
pub(crate) const ADD_RAFT_VOTER_RESULT_BYTES: usize = 4 * 1024;
pub(crate) const ADD_RAFT_VOTER_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct AddRaftVoterAdmission {
    pub(crate) observer: AddRaftVoterObserver,
    pub(crate) fault: Option<AddRaftVoterHostError>,
}

/// Exact validated core plan ready for protocol materialization and handoff.
pub(crate) struct AddRaftVoterSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    plan: AddRaftVoterPlan,
    result_limit: usize,
}

impl AddRaftVoterSubmission {
    pub(crate) fn into_parts(self) -> (OperationId, OperationDeadline, AddRaftVoterPlan, usize) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.result_limit,
        )
    }
}

pub(crate) enum AddRaftVoterTurn {
    Idle,
    Progress,
    Submit(AddRaftVoterSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AddRaftVoterHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct AddRaftVoterOperation {
    operation_id: OperationId,
    machine: AddRaftVoterMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<AddRaftVoterSubmission>,
    handoff: AddRaftVoterHandoff,
    call: Option<AddRaftVoterCall>,
    recovered_call: Option<RecoveredAddRaftVoterCall>,
    raw_terminal: Option<AddRaftVoterRawTerminal>,
    terminal: Option<AddRaftVoterTerminal>,
}

pub(crate) struct AddRaftVoterHost {
    operations: Vec<AddRaftVoterOperation>,
    completions: CompletionRegistry<AddRaftVoterTerminal, AdminAddRaftVoterPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<AddRaftVoterHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl AddRaftVoterHost {
    pub(crate) fn new(publisher: AdminAddRaftVoterPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(ADD_RAFT_VOTER_CAPACITY),
            completions: CompletionRegistry::with_publisher(ADD_RAFT_VOTER_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(ADD_RAFT_VOTER_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
        driver: Option<&crate::driver::DriverOwner>,
    ) -> Result<AddRaftVoterTurn, AddRaftVoterHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call(driver)? {
            return Ok(AddRaftVoterTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(AddRaftVoterTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, AddRaftVoterInput::DeadlineElapsed)?;
            return Ok(AddRaftVoterTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(AddRaftVoterHostError::MissingSubmission)?;
        self.operations[index].handoff = AddRaftVoterHandoff::HandedOff;
        Ok(AddRaftVoterTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: AddRaftVoterCall,
    ) -> Result<(), AddRaftVoterHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AddRaftVoterHostError::UnknownOperation)?;
        if self.operations[index].handoff != AddRaftVoterHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(AddRaftVoterHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, AddRaftVoterInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), AddRaftVoterHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AddRaftVoterHostError::UnknownOperation)?;
        if self.operations[index].handoff != AddRaftVoterHandoff::HandedOff {
            return Err(AddRaftVoterHostError::InvalidHandoff);
        }
        self.apply(operation_id, AddRaftVoterInput::DriverRejected)
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
        input: AddRaftVoterInput,
    ) -> Result<(), AddRaftVoterHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AddRaftVoterHostError::UnknownOperation)?;
        let accepted = matches!(&input, AddRaftVoterInput::DriverAccepted);
        if accepted && self.operations[index].handoff != AddRaftVoterHandoff::HandedOff {
            return Err(AddRaftVoterHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = AddRaftVoterHandoff::Submitted;
        }
        if let Some(AddRaftVoterEffect::Complete { terminal, .. }) = transition.into_effect() {
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
