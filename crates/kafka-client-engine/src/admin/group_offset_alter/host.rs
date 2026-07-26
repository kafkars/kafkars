//! Bounded ownership of accepted offset-alteration machines and tracked calls.

mod admission;
mod model;
#[cfg(test)]
mod ownership_test;
mod recovered;
#[cfg(test)]
mod recovered_test;
mod response;
mod terminal;

use crate::{
    admin::AlterConsumerGroupOffsetsPublisher,
    completion::{CompletionId, CompletionRegistry},
    driver::GroupOffsetAlterCall,
};
use kafka_client_core::{
    AlterConsumerGroupOffsetsEffect, AlterConsumerGroupOffsetsInput, AlterConsumerGroupOffsetsPlan,
    AlterConsumerGroupOffsetsTerminal, Moment, OperationId,
};

use super::{AlterConsumerGroupOffsetsHostError, AlterConsumerGroupOffsetsObserver};
use model::{
    AlterConsumerGroupOffsetsBounds, AlterConsumerGroupOffsetsHandoff,
    AlterConsumerGroupOffsetsOperation,
};
pub(crate) use model::{AlterConsumerGroupOffsetsSubmission, AlterConsumerGroupOffsetsTurn};

pub(crate) const ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY: usize = 16;
pub(crate) const ALTER_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct AlterConsumerGroupOffsetsAdmission {
    pub(crate) observer: AlterConsumerGroupOffsetsObserver,
    pub(crate) fault: Option<AlterConsumerGroupOffsetsHostError>,
}

pub(crate) struct AlterConsumerGroupOffsetsHost {
    operations: Vec<AlterConsumerGroupOffsetsOperation>,
    completions:
        CompletionRegistry<AlterConsumerGroupOffsetsTerminal, AlterConsumerGroupOffsetsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<AlterConsumerGroupOffsetsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl AlterConsumerGroupOffsetsHost {
    pub(crate) fn new(publisher: AlterConsumerGroupOffsetsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<AlterConsumerGroupOffsetsTurn, AlterConsumerGroupOffsetsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(AlterConsumerGroupOffsetsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(AlterConsumerGroupOffsetsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(
                operation_id,
                AlterConsumerGroupOffsetsInput::DeadlineElapsed,
            )?;
            return Ok(AlterConsumerGroupOffsetsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(AlterConsumerGroupOffsetsHostError::MissingSubmission)?;
        self.operations[index].mark_handed_off();
        Ok(AlterConsumerGroupOffsetsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: GroupOffsetAlterCall,
    ) -> Result<(), AlterConsumerGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AlterConsumerGroupOffsetsHostError::UnknownOperation)?;
        if self.operations[index].handoff != AlterConsumerGroupOffsetsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
            || self.operations[index].rejected_submission.is_some()
        {
            return Err(AlterConsumerGroupOffsetsHostError::InvalidHandoff);
        }
        let matches = self.operations[index].matches_call(&call);
        self.operations[index].call = Some(call);
        if !matches {
            return Err(AlterConsumerGroupOffsetsHostError::SubmissionMismatch);
        }
        self.apply(operation_id, AlterConsumerGroupOffsetsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        plan: AlterConsumerGroupOffsetsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Result<(), AlterConsumerGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AlterConsumerGroupOffsetsHostError::UnknownOperation)?;
        if self.operations[index].handoff != AlterConsumerGroupOffsetsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
            || self.operations[index].rejected_submission.is_some()
        {
            return Err(AlterConsumerGroupOffsetsHostError::InvalidHandoff);
        }
        let bounds = AlterConsumerGroupOffsetsBounds {
            request_scratch_limit,
            result_limit,
        };
        let matches = self.operations[index].matches_submission(&plan, bounds);
        self.operations[index].rejected_submission = Some((plan, bounds));
        if !matches {
            return Err(AlterConsumerGroupOffsetsHostError::SubmissionMismatch);
        }
        let transition = self.operations[index]
            .machine
            .apply(AlterConsumerGroupOffsetsInput::DriverRejected)?;
        let terminal = match transition.into_effect() {
            Some(AlterConsumerGroupOffsetsEffect::Complete {
                operation_id: effect_id,
                terminal,
            }) if effect_id == operation_id => terminal,
            _ => return Err(AlterConsumerGroupOffsetsHostError::MissingTerminal),
        };
        drop(self.operations[index].rejected_submission.take());
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
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
        input: AlterConsumerGroupOffsetsInput,
    ) -> Result<(), AlterConsumerGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AlterConsumerGroupOffsetsHostError::UnknownOperation)?;
        let accepted = matches!(&input, AlterConsumerGroupOffsetsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != AlterConsumerGroupOffsetsHandoff::HandedOff
        {
            return Err(AlterConsumerGroupOffsetsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].mark_submitted();
        }
        if let Some(AlterConsumerGroupOffsetsEffect::Complete {
            operation_id: effect_id,
            terminal,
        }) = transition.into_effect()
        {
            if effect_id != operation_id {
                return Err(AlterConsumerGroupOffsetsHostError::SubmissionMismatch);
            }
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
