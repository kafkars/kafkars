//! Bounded ownership of accepted `DeleteTopics` machines and terminal capacity.

mod admission;
mod terminal;

use core::fmt;

use kafka_client_core::{
    DeleteTopicsEffect, DeleteTopicsInput, DeleteTopicsMachine, DeleteTopicsMachineError,
    DeleteTopicsPlan, DeleteTopicsTerminal, Moment, OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry, CompletionRegistryError},
};

use super::{DeleteTopicsObserver, DeleteTopicsPublisher};

pub(crate) const DELETE_TOPICS_CAPACITY: usize = 32;
pub(crate) const DELETE_TOPICS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct DeleteTopicsAdmission {
    pub(crate) observer: DeleteTopicsObserver,
    pub(crate) fault: Option<DeleteTopicsHostError>,
}

pub(crate) struct DeleteTopicsSubmission {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: OperationDeadline,
    pub(crate) plan: DeleteTopicsPlan,
    pub(crate) retained_bytes: usize,
}

impl DeleteTopicsSubmission {
    pub(crate) fn into_parts(self) -> (OperationId, OperationDeadline, DeleteTopicsPlan, usize) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.retained_bytes,
        )
    }
}

pub(crate) enum DeleteTopicsTurn {
    Idle,
    Progress,
    Submit(DeleteTopicsSubmission),
}

pub(super) struct DeleteTopicsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: DeleteTopicsMachine,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) submission: Option<DeleteTopicsSubmission>,
    pub(super) terminal: Option<DeleteTopicsTerminal>,
}

pub(crate) struct DeleteTopicsHost {
    pub(super) operations: Vec<DeleteTopicsOperation>,
    pub(super) completions: CompletionRegistry<DeleteTopicsTerminal, DeleteTopicsPublisher>,
    pub(super) next_operation_id: Option<OperationId>,
    pub(super) reclaim_pending: Option<CompletionId>,
    pub(super) retained_bytes: usize,
    pub(super) accepting: bool,
    pub(super) health: Option<DeleteTopicsHostError>,
    pub(super) published_bytes: Vec<(CompletionId, usize)>,
}

impl DeleteTopicsHost {
    pub(crate) fn new(publisher: DeleteTopicsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DELETE_TOPICS_CAPACITY),
            completions: CompletionRegistry::with_publisher(DELETE_TOPICS_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DELETE_TOPICS_CAPACITY),
        }
    }

    pub(crate) fn turn(&mut self, now: Moment) -> Result<DeleteTopicsTurn, DeleteTopicsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? {
            return Ok(DeleteTopicsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DeleteTopicsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].machine_id();
            self.apply(operation_id, DeleteTopicsInput::DeadlineElapsed)?;
            return Ok(DeleteTopicsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DeleteTopicsHostError::MissingSubmission)?;
        Ok(DeleteTopicsTurn::Submit(submission))
    }

    pub(crate) fn apply(
        &mut self,
        operation_id: OperationId,
        input: DeleteTopicsInput,
    ) -> Result<(), DeleteTopicsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DeleteTopicsHostError::UnknownOperation)?;
        let transition = self.operations[index].machine.apply(input)?;
        if let Some(DeleteTopicsEffect::Complete { terminal, .. }) = transition.into_effect() {
            self.operations[index].terminal = Some(terminal);
            self.publish_terminal(index)?;
        }
        Ok(())
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

    pub(super) fn operation_index(&self, operation_id: OperationId) -> Option<usize> {
        self.operations
            .iter()
            .position(|operation| operation.machine_id() == operation_id)
    }
}

impl DeleteTopicsOperation {
    const fn machine_id(&self) -> OperationId {
        self.operation_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteTopicsHostError {
    Machine(DeleteTopicsMachineError),
    Completion(CompletionRegistryError),
    UnknownOperation,
    MissingSubmission,
    MissingTerminal,
    ByteAccounting,
    Unsettled(usize),
    Wake,
}

impl From<DeleteTopicsMachineError> for DeleteTopicsHostError {
    fn from(error: DeleteTopicsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DeleteTopicsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DeleteTopicsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "DeleteTopics host invariant failed: {self:?}")
    }
}

impl std::error::Error for DeleteTopicsHostError {}
