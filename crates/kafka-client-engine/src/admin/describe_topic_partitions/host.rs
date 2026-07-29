//! Bounded ownership of topic-partition page machines and concrete calls.

mod admission;
mod effect;
mod model;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    DescribeTopicPartitionsInput, DescribeTopicPartitionsMachine, DescribeTopicPartitionsTerminal,
    Moment, OperationId,
};

use crate::{
    admin::AdminDescribeTopicPartitionsPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{DescribeTopicPartitionsCall, DescribeTopicPartitionsRawTerminal},
};

use super::{AdminDescribeTopicPartitionsHostError, AdminDescribeTopicPartitionsObserver};

use model::AdminDescribeTopicPartitionsHandoff;
pub(crate) use model::{AdminDescribeTopicPartitionsSubmission, AdminDescribeTopicPartitionsTurn};

pub(crate) const ADMIN_DESCRIBE_TOPIC_PARTITIONS_CAPACITY: usize = 16;
pub(super) const ADMIN_DESCRIBE_TOPIC_PARTITIONS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct AdminDescribeTopicPartitionsAdmission {
    pub(crate) observer: AdminDescribeTopicPartitionsObserver,
    pub(crate) fault: Option<AdminDescribeTopicPartitionsHostError>,
}

struct AdminDescribeTopicPartitionsOperation {
    operation_id: OperationId,
    machine: DescribeTopicPartitionsMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<AdminDescribeTopicPartitionsSubmission>,
    handoff: AdminDescribeTopicPartitionsHandoff,
    call: Option<DescribeTopicPartitionsCall>,
    recovered_call: Option<crate::driver::RecoveredDescribeTopicPartitionsCall>,
    raw_terminal: Option<DescribeTopicPartitionsRawTerminal>,
    terminal: Option<DescribeTopicPartitionsTerminal>,
}

pub(crate) struct AdminDescribeTopicPartitionsHost {
    operations: Vec<AdminDescribeTopicPartitionsOperation>,
    completions:
        CompletionRegistry<DescribeTopicPartitionsTerminal, AdminDescribeTopicPartitionsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<AdminDescribeTopicPartitionsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl AdminDescribeTopicPartitionsHost {
    pub(crate) fn new(publisher: AdminDescribeTopicPartitionsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(ADMIN_DESCRIBE_TOPIC_PARTITIONS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                ADMIN_DESCRIBE_TOPIC_PARTITIONS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(ADMIN_DESCRIBE_TOPIC_PARTITIONS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<AdminDescribeTopicPartitionsTurn, AdminDescribeTopicPartitionsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(AdminDescribeTopicPartitionsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(AdminDescribeTopicPartitionsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, DescribeTopicPartitionsInput::DeadlineElapsed)?;
            return Ok(AdminDescribeTopicPartitionsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(AdminDescribeTopicPartitionsHostError::MissingSubmission)?;
        self.operations[index].handoff = AdminDescribeTopicPartitionsHandoff::HandedOff;
        Ok(AdminDescribeTopicPartitionsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: DescribeTopicPartitionsCall,
    ) -> Result<(), AdminDescribeTopicPartitionsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AdminDescribeTopicPartitionsHostError::UnknownOperation)?;
        if self.operations[index].handoff != AdminDescribeTopicPartitionsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
        {
            return Err(AdminDescribeTopicPartitionsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, DescribeTopicPartitionsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), AdminDescribeTopicPartitionsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AdminDescribeTopicPartitionsHostError::UnknownOperation)?;
        if self.operations[index].handoff != AdminDescribeTopicPartitionsHandoff::HandedOff
            || self.operations[index].recovered_call.is_some()
        {
            return Err(AdminDescribeTopicPartitionsHostError::InvalidHandoff);
        }
        self.apply(operation_id, DescribeTopicPartitionsInput::DriverRejected)
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
        input: DescribeTopicPartitionsInput,
    ) -> Result<(), AdminDescribeTopicPartitionsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AdminDescribeTopicPartitionsHostError::UnknownOperation)?;
        let accepted = matches!(&input, DescribeTopicPartitionsInput::DriverAccepted);
        if accepted
            && self.operations[index].handoff != AdminDescribeTopicPartitionsHandoff::HandedOff
        {
            return Err(AdminDescribeTopicPartitionsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = AdminDescribeTopicPartitionsHandoff::Submitted;
        }
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }
}
