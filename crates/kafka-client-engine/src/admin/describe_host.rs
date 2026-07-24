//! Bounded ownership of accepted `DescribeCluster` machines and terminals.

mod admission;
mod terminal;

use core::fmt;

use kafka_client_core::{
    DescribeClusterEffect, DescribeClusterInput, DescribeClusterMachine,
    DescribeClusterMachineError, DescribeClusterTerminal, Moment, OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry, CompletionRegistryError},
};

use super::{DescribeClusterObserver, completion::DescribeClusterPublisher};

pub(crate) const DESCRIBE_CLUSTER_CAPACITY: usize = 16;
const DESCRIBE_CLUSTER_RETAINED_BYTES: usize = 4 * 1024 * 1024;
const DESCRIBE_CLUSTER_OPERATION_BYTES: usize = 128 * 1024;

pub(crate) struct DescribeClusterAdmission {
    pub(crate) observer: DescribeClusterObserver,
    pub(crate) fault: Option<DescribeClusterHostError>,
}

pub(crate) struct DescribeClusterSubmission {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: OperationDeadline,
    pub(crate) retained_bytes: usize,
    pub(crate) include_fenced_brokers: bool,
    pub(crate) include_authorized_operations: bool,
}

impl DescribeClusterSubmission {
    pub(crate) const fn into_parts(self) -> (OperationId, OperationDeadline, usize, bool, bool) {
        (
            self.operation_id,
            self.deadline,
            self.retained_bytes,
            self.include_fenced_brokers,
            self.include_authorized_operations,
        )
    }
}

pub(crate) enum DescribeClusterTurn {
    Idle,
    Progress,
    Submit(DescribeClusterSubmission),
}

pub(super) struct DescribeClusterOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: DescribeClusterMachine,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) submission: Option<DescribeClusterSubmission>,
    pub(super) terminal: Option<DescribeClusterTerminal>,
}

pub(crate) struct DescribeClusterHost {
    pub(super) operations: Vec<DescribeClusterOperation>,
    pub(super) completions: CompletionRegistry<DescribeClusterTerminal, DescribeClusterPublisher>,
    pub(super) next_operation_id: Option<OperationId>,
    pub(super) reclaim_pending: Option<CompletionId>,
    pub(super) retained_bytes: usize,
    pub(super) accepting: bool,
    pub(super) health: Option<DescribeClusterHostError>,
    pub(super) published_bytes: Vec<(CompletionId, usize)>,
}

impl DescribeClusterHost {
    pub(crate) fn new(publisher: DescribeClusterPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DESCRIBE_CLUSTER_CAPACITY),
            completions: CompletionRegistry::with_publisher(DESCRIBE_CLUSTER_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DESCRIBE_CLUSTER_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<DescribeClusterTurn, DescribeClusterHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? {
            return Ok(DescribeClusterTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DescribeClusterTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, DescribeClusterInput::DeadlineElapsed)?;
            return Ok(DescribeClusterTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DescribeClusterHostError::MissingSubmission)?;
        Ok(DescribeClusterTurn::Submit(submission))
    }

    pub(crate) fn apply(
        &mut self,
        operation_id: OperationId,
        input: DescribeClusterInput,
    ) -> Result<(), DescribeClusterHostError> {
        let index = self
            .operations
            .iter()
            .position(|operation| operation.operation_id == operation_id)
            .ok_or(DescribeClusterHostError::UnknownOperation)?;
        let transition = self.operations[index].machine.apply(input)?;
        if let Some(DescribeClusterEffect::Complete { terminal, .. }) = transition.into_effect() {
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeClusterHostError {
    Machine(DescribeClusterMachineError),
    Completion(CompletionRegistryError),
    UnknownOperation,
    MissingSubmission,
    MissingTerminal,
    ByteAccounting,
    Unsettled(usize),
    Wake,
}

impl From<DescribeClusterMachineError> for DescribeClusterHostError {
    fn from(error: DescribeClusterMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DescribeClusterHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DescribeClusterHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "DescribeCluster host invariant failed: {self:?}")
    }
}

impl std::error::Error for DescribeClusterHostError {}
