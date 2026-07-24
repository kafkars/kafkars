//! Bounded ownership of accepted `CreatePartitions` machines and capacity.

mod admission;
mod terminal;

use core::fmt;

use kafka_client_core::{
    CreatePartitionsEffect, CreatePartitionsInput, CreatePartitionsMachine,
    CreatePartitionsMachineError, CreatePartitionsPlan, CreatePartitionsTerminal, Moment,
    OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry, CompletionRegistryError},
};

use super::{CreatePartitionsObserver, completion::CreatePartitionsPublisher};

pub(crate) const CREATE_PARTITIONS_CAPACITY: usize = 32;
pub(crate) const CREATE_PARTITIONS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct CreatePartitionsAdmission {
    pub(crate) observer: CreatePartitionsObserver,
    pub(crate) fault: Option<CreatePartitionsHostError>,
}

pub(crate) struct CreatePartitionsSubmission {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: OperationDeadline,
    pub(crate) plan: CreatePartitionsPlan,
    pub(crate) retained_bytes: usize,
}

impl CreatePartitionsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (OperationId, OperationDeadline, CreatePartitionsPlan, usize) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.retained_bytes,
        )
    }
}

pub(crate) enum CreatePartitionsTurn {
    Idle,
    Progress,
    Submit(CreatePartitionsSubmission),
}

pub(super) struct CreatePartitionsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: CreatePartitionsMachine,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) submission: Option<CreatePartitionsSubmission>,
    pub(super) terminal: Option<CreatePartitionsTerminal>,
}

pub(crate) struct CreatePartitionsHost {
    pub(super) operations: Vec<CreatePartitionsOperation>,
    pub(super) completions: CompletionRegistry<CreatePartitionsTerminal, CreatePartitionsPublisher>,
    pub(super) next_operation_id: Option<OperationId>,
    pub(super) reclaim_pending: Option<CompletionId>,
    pub(super) retained_bytes: usize,
    pub(super) accepting: bool,
    pub(super) health: Option<CreatePartitionsHostError>,
    pub(super) published_bytes: Vec<(CompletionId, usize)>,
}

impl CreatePartitionsHost {
    pub(crate) fn new(publisher: CreatePartitionsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(CREATE_PARTITIONS_CAPACITY),
            completions: CompletionRegistry::with_publisher(CREATE_PARTITIONS_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(CREATE_PARTITIONS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<CreatePartitionsTurn, CreatePartitionsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? {
            return Ok(CreatePartitionsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(CreatePartitionsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].machine_id();
            self.apply(operation_id, CreatePartitionsInput::DeadlineElapsed)?;
            return Ok(CreatePartitionsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(CreatePartitionsHostError::MissingSubmission)?;
        Ok(CreatePartitionsTurn::Submit(submission))
    }

    pub(crate) fn apply(
        &mut self,
        operation_id: OperationId,
        input: CreatePartitionsInput,
    ) -> Result<(), CreatePartitionsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(CreatePartitionsHostError::UnknownOperation)?;
        let transition = self.operations[index].machine.apply(input)?;
        if let Some(CreatePartitionsEffect::Complete { terminal, .. }) = transition.into_effect() {
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

impl CreatePartitionsOperation {
    const fn machine_id(&self) -> OperationId {
        self.operation_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreatePartitionsHostError {
    Machine(CreatePartitionsMachineError),
    Completion(CompletionRegistryError),
    UnknownOperation,
    MissingSubmission,
    MissingTerminal,
    ByteAccounting,
    Unsettled(usize),
    Wake,
}

impl From<CreatePartitionsMachineError> for CreatePartitionsHostError {
    fn from(error: CreatePartitionsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for CreatePartitionsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for CreatePartitionsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "CreatePartitions host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for CreatePartitionsHostError {}
