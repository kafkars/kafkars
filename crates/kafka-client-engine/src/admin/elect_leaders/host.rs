//! Bounded ownership of accepted election machines and tracked calls.

mod admission;
mod recovered;
mod response;
mod terminal;

use kafka_client_core::{
    ElectLeadersEffect, ElectLeadersInput, ElectLeadersMachine, ElectLeadersPlan,
    ElectLeadersTerminal as CoreTerminal, Moment, OperationId,
};

use crate::{
    admin::ElectLeadersPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{ElectLeadersCall, ElectLeadersTerminal, RecoveredElectLeadersCall},
};

use super::{ElectLeadersHostError, ElectLeadersObserver};

pub(crate) const ELECT_LEADERS_CAPACITY: usize = 16;
pub(crate) const ELECT_LEADERS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct ElectLeadersAdmission {
    pub(crate) observer: ElectLeadersObserver,
    pub(crate) fault: Option<ElectLeadersHostError>,
}

pub(crate) struct ElectLeadersSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    plan: ElectLeadersPlan,
    request_scratch_limit: usize,
}

pub(crate) enum ElectLeadersTurn {
    Idle,
    Progress,
    Submit(ElectLeadersSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ElectLeadersHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct ElectLeadersOperation {
    operation_id: OperationId,
    machine: ElectLeadersMachine,
    response_plan: ElectLeadersPlan,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    result_limit: usize,
    submission: Option<ElectLeadersSubmission>,
    handoff: ElectLeadersHandoff,
    call: Option<ElectLeadersCall>,
    recovered_call: Option<RecoveredElectLeadersCall>,
    raw_terminal: Option<ElectLeadersTerminal>,
    terminal: Option<CoreTerminal>,
}

pub(crate) struct ElectLeadersHost {
    operations: Vec<ElectLeadersOperation>,
    completions: CompletionRegistry<CoreTerminal, ElectLeadersPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<ElectLeadersHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl ElectLeadersHost {
    pub(crate) fn new(publisher: ElectLeadersPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(ELECT_LEADERS_CAPACITY),
            completions: CompletionRegistry::with_publisher(ELECT_LEADERS_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(ELECT_LEADERS_CAPACITY),
        }
    }

    pub(crate) fn turn(&mut self, now: Moment) -> Result<ElectLeadersTurn, ElectLeadersHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(ElectLeadersTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(ElectLeadersTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, ElectLeadersInput::DeadlineElapsed)?;
            return Ok(ElectLeadersTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(ElectLeadersHostError::MissingSubmission)?;
        self.operations[index].handoff = ElectLeadersHandoff::HandedOff;
        Ok(ElectLeadersTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: ElectLeadersCall,
    ) -> Result<(), ElectLeadersHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ElectLeadersHostError::UnknownOperation)?;
        if self.operations[index].handoff != ElectLeadersHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(ElectLeadersHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, ElectLeadersInput::DriverAccepted)?;
        self.operations[index].handoff = ElectLeadersHandoff::Submitted;
        Ok(())
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), ElectLeadersHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ElectLeadersHostError::UnknownOperation)?;
        if self.operations[index].handoff != ElectLeadersHandoff::HandedOff {
            return Err(ElectLeadersHostError::InvalidHandoff);
        }
        self.apply(operation_id, ElectLeadersInput::DriverRejected)
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
        input: ElectLeadersInput,
    ) -> Result<(), ElectLeadersHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ElectLeadersHostError::UnknownOperation)?;
        let transition = self.operations[index].machine.apply(input)?;
        if let Some(ElectLeadersEffect::Complete { terminal, .. }) = transition.into_effect() {
            self.operations[index].terminal = Some(terminal);
            self.publish_terminal(index)?;
        }
        Ok(())
    }
}
