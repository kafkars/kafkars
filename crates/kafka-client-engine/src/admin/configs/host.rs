//! Bounded ownership for accepted `DescribeConfigs` machines and terminal capacity.

mod admission;
mod terminal;

use core::fmt;

use kafka_client_core::{
    DescribeConfigsEffect, DescribeConfigsInput, DescribeConfigsMachine,
    DescribeConfigsMachineError, DescribeConfigsPlan, DescribeConfigsTerminal, Moment, OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry, CompletionRegistryError},
};

use super::DescribeConfigsObserver;
use crate::admin::DescribeConfigsPublisher;

pub(crate) const DESCRIBE_CONFIGS_CAPACITY: usize = 16;
pub(crate) const DESCRIBE_CONFIGS_RETAINED_BYTES: usize = 8 * 1024 * 1024;

pub(crate) struct DescribeConfigsAdmission {
    pub(crate) observer: DescribeConfigsObserver,
    pub(crate) fault: Option<DescribeConfigsHostError>,
}

pub(crate) struct DescribeConfigsSubmission {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: OperationDeadline,
    pub(crate) plan: DescribeConfigsPlan,
    pub(crate) result_limit: usize,
}

impl DescribeConfigsSubmission {
    pub(crate) fn into_parts(self) -> (OperationId, OperationDeadline, DescribeConfigsPlan, usize) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.result_limit,
        )
    }
}

pub(crate) enum DescribeConfigsTurn {
    Idle,
    Progress,
    Submit(DescribeConfigsSubmission),
}

pub(super) struct DescribeConfigsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: DescribeConfigsMachine,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) result_limit: usize,
    pub(super) submission: Option<DescribeConfigsSubmission>,
    pub(super) terminal: Option<DescribeConfigsTerminal>,
}

pub(crate) struct DescribeConfigsHost {
    pub(super) operations: Vec<DescribeConfigsOperation>,
    pub(super) completions: CompletionRegistry<DescribeConfigsTerminal, DescribeConfigsPublisher>,
    pub(super) next_operation_id: Option<OperationId>,
    pub(super) reclaim_pending: Option<CompletionId>,
    pub(super) retained_bytes: usize,
    pub(super) accepting: bool,
    pub(super) health: Option<DescribeConfigsHostError>,
    pub(super) published_bytes: Vec<(CompletionId, usize)>,
}

impl DescribeConfigsHost {
    pub(crate) fn new(publisher: DescribeConfigsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DESCRIBE_CONFIGS_CAPACITY),
            completions: CompletionRegistry::with_publisher(DESCRIBE_CONFIGS_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DESCRIBE_CONFIGS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<DescribeConfigsTurn, DescribeConfigsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? {
            return Ok(DescribeConfigsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DescribeConfigsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].machine_id();
            self.apply(operation_id, DescribeConfigsInput::DeadlineElapsed)?;
            return Ok(DescribeConfigsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DescribeConfigsHostError::MissingSubmission)?;
        Ok(DescribeConfigsTurn::Submit(submission))
    }

    pub(crate) fn apply(
        &mut self,
        operation_id: OperationId,
        input: DescribeConfigsInput,
    ) -> Result<(), DescribeConfigsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeConfigsHostError::UnknownOperation)?;
        let transition = self.operations[index].machine.apply(input)?;
        if let Some(DescribeConfigsEffect::Complete { terminal, .. }) = transition.into_effect() {
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

impl DescribeConfigsOperation {
    const fn machine_id(&self) -> OperationId {
        self.operation_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeConfigsHostError {
    Machine(DescribeConfigsMachineError),
    Completion(CompletionRegistryError),
    UnknownOperation,
    MissingSubmission,
    MissingTerminal,
    ByteAccounting,
    Unsettled(usize),
    Wake,
}

impl From<DescribeConfigsMachineError> for DescribeConfigsHostError {
    fn from(error: DescribeConfigsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DescribeConfigsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DescribeConfigsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "DescribeConfigs host invariant failed: {self:?}")
    }
}

impl std::error::Error for DescribeConfigsHostError {}
