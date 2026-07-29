//! Bounded ownership of accepted offset-deletion machines and tracked calls.

mod admission;
mod model;
mod response;
mod terminal;

#[cfg(test)]
mod ownership_test;

use kafka_client_core::{
    DeleteConsumerGroupOffsetsEffect, DeleteConsumerGroupOffsetsInput,
    DeleteConsumerGroupOffsetsPlan, DeleteConsumerGroupOffsetsTerminal, Moment, OperationId,
};

use crate::{
    admin::DeleteConsumerGroupOffsetsPublisher,
    completion::{CompletionId, CompletionRegistry},
    driver::GroupOffsetDeleteCall,
};

use super::{DeleteConsumerGroupOffsetsHostError, DeleteConsumerGroupOffsetsObserver};
use model::{DeleteConsumerGroupOffsetsHandoff, DeleteConsumerGroupOffsetsOperation};
pub(crate) use model::{DeleteConsumerGroupOffsetsSubmission, DeleteConsumerGroupOffsetsTurn};

pub(crate) const DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY: usize = 16;
pub(crate) const DELETE_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct DeleteConsumerGroupOffsetsAdmission {
    pub(crate) observer: DeleteConsumerGroupOffsetsObserver,
    pub(crate) fault: Option<DeleteConsumerGroupOffsetsHostError>,
}

pub(crate) struct DeleteConsumerGroupOffsetsHost {
    operations: Vec<DeleteConsumerGroupOffsetsOperation>,
    completions:
        CompletionRegistry<DeleteConsumerGroupOffsetsTerminal, DeleteConsumerGroupOffsetsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<DeleteConsumerGroupOffsetsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl DeleteConsumerGroupOffsetsHost {
    pub(crate) fn new(publisher: DeleteConsumerGroupOffsetsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<DeleteConsumerGroupOffsetsTurn, DeleteConsumerGroupOffsetsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(DeleteConsumerGroupOffsetsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DeleteConsumerGroupOffsetsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(
                operation_id,
                DeleteConsumerGroupOffsetsInput::DeadlineElapsed,
            )?;
            return Ok(DeleteConsumerGroupOffsetsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DeleteConsumerGroupOffsetsHostError::MissingSubmission)?;
        self.operations[index].mark_handed_off();
        Ok(DeleteConsumerGroupOffsetsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: GroupOffsetDeleteCall,
    ) -> Result<(), DeleteConsumerGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DeleteConsumerGroupOffsetsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DeleteConsumerGroupOffsetsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
        {
            return Err(DeleteConsumerGroupOffsetsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        let operation = &self.operations[index];
        if !operation.call.as_ref().is_some_and(|call| {
            call.matches_evidence(&operation.response_plan, operation.result_limit)
        }) {
            return Err(DeleteConsumerGroupOffsetsHostError::SubmissionMismatch);
        }
        self.apply(
            operation_id,
            DeleteConsumerGroupOffsetsInput::DriverAccepted,
        )
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        plan: DeleteConsumerGroupOffsetsPlan,
        result_limit: usize,
    ) -> Result<(), DeleteConsumerGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DeleteConsumerGroupOffsetsHostError::UnknownOperation)?;
        let operation = &self.operations[index];
        if operation.handoff != DeleteConsumerGroupOffsetsHandoff::HandedOff
            || operation.call.is_some()
            || operation.recovered_call.is_some()
            || operation.raw_terminal.is_some()
        {
            return Err(DeleteConsumerGroupOffsetsHostError::InvalidHandoff);
        }
        if operation.response_plan != plan || operation.result_limit != result_limit {
            return Err(DeleteConsumerGroupOffsetsHostError::SubmissionMismatch);
        }
        self.apply(
            operation_id,
            DeleteConsumerGroupOffsetsInput::DriverRejected,
        )
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
        input: DeleteConsumerGroupOffsetsInput,
    ) -> Result<(), DeleteConsumerGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DeleteConsumerGroupOffsetsHostError::UnknownOperation)?;
        let accepted = matches!(&input, DeleteConsumerGroupOffsetsInput::DriverAccepted);
        if accepted
            && self.operations[index].handoff != DeleteConsumerGroupOffsetsHandoff::HandedOff
        {
            return Err(DeleteConsumerGroupOffsetsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].mark_submitted();
        }
        if let Some(DeleteConsumerGroupOffsetsEffect::Complete { terminal, .. }) =
            transition.into_effect()
        {
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
