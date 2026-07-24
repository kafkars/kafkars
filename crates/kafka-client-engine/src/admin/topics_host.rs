//! Bounded ownership of accepted `DescribeTopics` machines and terminal capacity.

mod admission;
mod terminal;

use core::fmt;

use kafka_client_core::{
    DescribeTopicsEffect, DescribeTopicsInput, DescribeTopicsMachine, DescribeTopicsMachineError,
    DescribeTopicsPlan, DescribeTopicsTerminal, Moment, OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry, CompletionRegistryError},
};

use super::{DescribeTopicsObserver, DescribeTopicsPublisher};

pub(crate) const DESCRIBE_TOPICS_CAPACITY: usize = 32;
pub(crate) const DESCRIBE_TOPICS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct DescribeTopicsAdmission {
    pub(crate) observer: DescribeTopicsObserver,
    pub(crate) fault: Option<DescribeTopicsHostError>,
}

pub(crate) struct DescribeTopicsSubmission {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: OperationDeadline,
    pub(crate) plan: DescribeTopicsPlan,
    pub(crate) retained_bytes: usize,
}

impl DescribeTopicsSubmission {
    pub(crate) fn into_parts(self) -> (OperationId, OperationDeadline, DescribeTopicsPlan, usize) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.retained_bytes,
        )
    }
}

pub(crate) enum DescribeTopicsTurn {
    Idle,
    Progress,
    Submit(DescribeTopicsSubmission),
}

pub(super) struct DescribeTopicsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: DescribeTopicsMachine,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) submission: Option<DescribeTopicsSubmission>,
    pub(super) terminal: Option<DescribeTopicsTerminal>,
}

pub(crate) struct DescribeTopicsHost {
    pub(super) operations: Vec<DescribeTopicsOperation>,
    pub(super) completions: CompletionRegistry<DescribeTopicsTerminal, DescribeTopicsPublisher>,
    pub(super) next_operation_id: Option<OperationId>,
    pub(super) reclaim_pending: Option<CompletionId>,
    pub(super) retained_bytes: usize,
    pub(super) accepting: bool,
    pub(super) health: Option<DescribeTopicsHostError>,
    pub(super) published_bytes: Vec<(CompletionId, usize)>,
}

impl DescribeTopicsHost {
    pub(crate) fn new(publisher: DescribeTopicsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DESCRIBE_TOPICS_CAPACITY),
            completions: CompletionRegistry::with_publisher(DESCRIBE_TOPICS_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DESCRIBE_TOPICS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<DescribeTopicsTurn, DescribeTopicsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? {
            return Ok(DescribeTopicsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DescribeTopicsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].machine_id();
            self.apply(operation_id, DescribeTopicsInput::DeadlineElapsed)?;
            return Ok(DescribeTopicsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DescribeTopicsHostError::MissingSubmission)?;
        Ok(DescribeTopicsTurn::Submit(submission))
    }

    pub(crate) fn apply(
        &mut self,
        operation_id: OperationId,
        input: DescribeTopicsInput,
    ) -> Result<(), DescribeTopicsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeTopicsHostError::UnknownOperation)?;
        let transition = self.operations[index].machine.apply(input)?;
        if let Some(DescribeTopicsEffect::Complete { terminal, .. }) = transition.into_effect() {
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

impl DescribeTopicsOperation {
    const fn machine_id(&self) -> OperationId {
        self.operation_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeTopicsHostError {
    Machine(DescribeTopicsMachineError),
    Completion(CompletionRegistryError),
    UnknownOperation,
    MissingSubmission,
    MissingTerminal,
    ByteAccounting,
    Unsettled(usize),
    Wake,
}

impl From<DescribeTopicsMachineError> for DescribeTopicsHostError {
    fn from(error: DescribeTopicsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DescribeTopicsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DescribeTopicsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "DescribeTopics host invariant failed: {self:?}")
    }
}

impl std::error::Error for DescribeTopicsHostError {}
