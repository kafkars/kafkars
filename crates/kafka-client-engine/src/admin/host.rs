//! Bounded ownership of accepted `CreateTopics` machines and terminal capacity.

mod admission;
mod terminal;

use core::fmt;

use kafka_client_core::{
    CreateTopicsEffect, CreateTopicsInput, CreateTopicsMachine, CreateTopicsMachineError,
    CreateTopicsPlan, CreateTopicsTerminal, Moment, OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry, CompletionRegistryError},
};

use super::CreateTopicsObserver;

pub(crate) const CREATE_TOPICS_CAPACITY: usize = 32;
pub(crate) const CREATE_TOPICS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct CreateTopicsAdmission {
    pub(crate) observer: CreateTopicsObserver,
    pub(crate) fault: Option<CreateTopicsHostError>,
}

pub(crate) struct CreateTopicsSubmission {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: OperationDeadline,
    pub(crate) plan: CreateTopicsPlan,
    pub(crate) retained_bytes: usize,
}

impl CreateTopicsSubmission {
    pub(crate) fn into_parts(self) -> (OperationId, OperationDeadline, CreateTopicsPlan, usize) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.retained_bytes,
        )
    }
}

pub(crate) enum CreateTopicsTurn {
    Idle,
    Progress,
    Submit(CreateTopicsSubmission),
}

pub(super) struct CreateTopicsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: CreateTopicsMachine,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) submission: Option<CreateTopicsSubmission>,
    pub(super) terminal: Option<CreateTopicsTerminal>,
}

pub(crate) struct CreateTopicsHost {
    pub(super) operations: Vec<CreateTopicsOperation>,
    pub(super) completions: CompletionRegistry<CreateTopicsTerminal>,
    pub(super) next_operation_id: Option<OperationId>,
    pub(super) reclaim_pending: Option<CompletionId>,
    pub(super) retained_bytes: usize,
    pub(super) accepting: bool,
    pub(super) health: Option<CreateTopicsHostError>,
    pub(super) published_bytes: Vec<(CompletionId, usize)>,
}

impl CreateTopicsHost {
    pub(crate) fn new() -> Result<Self, std::io::Error> {
        Ok(Self {
            operations: Vec::with_capacity(CREATE_TOPICS_CAPACITY),
            completions: CompletionRegistry::start(CREATE_TOPICS_CAPACITY)?,
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(CREATE_TOPICS_CAPACITY),
        })
    }

    pub(crate) fn turn(&mut self, now: Moment) -> Result<CreateTopicsTurn, CreateTopicsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? {
            return Ok(CreateTopicsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(CreateTopicsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].machine_id();
            self.apply(operation_id, CreateTopicsInput::DeadlineElapsed)?;
            return Ok(CreateTopicsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(CreateTopicsHostError::MissingSubmission)?;
        Ok(CreateTopicsTurn::Submit(submission))
    }

    pub(crate) fn apply(
        &mut self,
        operation_id: OperationId,
        input: CreateTopicsInput,
    ) -> Result<(), CreateTopicsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(CreateTopicsHostError::UnknownOperation)?;
        let transition = self.operations[index].machine.apply(input)?;
        if let Some(CreateTopicsEffect::Complete { terminal, .. }) = transition.into_effect() {
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

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }

    pub(super) fn operation_index(&self, operation_id: OperationId) -> Option<usize> {
        self.operations
            .iter()
            .position(|operation| operation.machine_id() == operation_id)
    }
}

impl CreateTopicsOperation {
    const fn machine_id(&self) -> OperationId {
        self.operation_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreateTopicsHostError {
    Machine(CreateTopicsMachineError),
    Completion(CompletionRegistryError),
    UnknownOperation,
    MissingSubmission,
    MissingTerminal,
    ByteAccounting,
    Unsettled(usize),
    Wake,
}

impl From<CreateTopicsMachineError> for CreateTopicsHostError {
    fn from(error: CreateTopicsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for CreateTopicsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for CreateTopicsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CreateTopics host invariant failed: {self:?}")
    }
}

impl std::error::Error for CreateTopicsHostError {}
