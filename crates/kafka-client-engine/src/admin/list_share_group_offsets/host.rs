//! Bounded ownership of share-group offset machines and concrete calls.

mod admission;
mod model;
mod operation;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    ListShareGroupOffsetsEffect, ListShareGroupOffsetsInput, ListShareGroupOffsetsTerminal, Moment,
    OperationId,
};

use crate::{
    admin::AdminListShareGroupOffsetsPublisher,
    completion::{CompletionId, CompletionRegistry},
    driver::ListShareGroupOffsetsCall,
};

use super::{ListShareGroupOffsetsHostError, ListShareGroupOffsetsObserver};

use model::ListShareGroupOffsetsHandoff;
pub(crate) use model::{ListShareGroupOffsetsSubmission, ListShareGroupOffsetsTurn};
use operation::ListShareGroupOffsetsOperation;

pub(crate) const LIST_SHARE_GROUP_OFFSETS_CAPACITY: usize = 16;
pub(crate) const LIST_SHARE_GROUP_OFFSETS_RESULT_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const LIST_SHARE_GROUP_OFFSETS_RETAINED_BYTES: usize = 8 * 1024 * 1024;

pub(crate) struct ListShareGroupOffsetsAdmission {
    pub(crate) observer: ListShareGroupOffsetsObserver,
    pub(crate) fault: Option<ListShareGroupOffsetsHostError>,
}

pub(crate) struct ListShareGroupOffsetsHost {
    operations: Vec<ListShareGroupOffsetsOperation>,
    completions:
        CompletionRegistry<ListShareGroupOffsetsTerminal, AdminListShareGroupOffsetsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<ListShareGroupOffsetsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl ListShareGroupOffsetsHost {
    pub(crate) fn new(publisher: AdminListShareGroupOffsetsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(LIST_SHARE_GROUP_OFFSETS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                LIST_SHARE_GROUP_OFFSETS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(LIST_SHARE_GROUP_OFFSETS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<ListShareGroupOffsetsTurn, ListShareGroupOffsetsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(ListShareGroupOffsetsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(ListShareGroupOffsetsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, ListShareGroupOffsetsInput::DeadlineElapsed)?;
            return Ok(ListShareGroupOffsetsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(ListShareGroupOffsetsHostError::MissingSubmission)?;
        self.operations[index].handoff = ListShareGroupOffsetsHandoff::HandedOff;
        Ok(ListShareGroupOffsetsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: ListShareGroupOffsetsCall,
    ) -> Result<(), ListShareGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListShareGroupOffsetsHostError::UnknownOperation)?;
        if self.operations[index].handoff != ListShareGroupOffsetsHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(ListShareGroupOffsetsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, ListShareGroupOffsetsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), ListShareGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListShareGroupOffsetsHostError::UnknownOperation)?;
        if self.operations[index].handoff != ListShareGroupOffsetsHandoff::HandedOff {
            return Err(ListShareGroupOffsetsHostError::InvalidHandoff);
        }
        self.apply(operation_id, ListShareGroupOffsetsInput::DriverRejected)
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
        input: ListShareGroupOffsetsInput,
    ) -> Result<(), ListShareGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListShareGroupOffsetsHostError::UnknownOperation)?;
        let accepted = matches!(&input, ListShareGroupOffsetsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != ListShareGroupOffsetsHandoff::HandedOff {
            return Err(ListShareGroupOffsetsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = ListShareGroupOffsetsHandoff::Submitted;
        }
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }

    fn install_effect(
        &mut self,
        index: usize,
        effect: ListShareGroupOffsetsEffect,
    ) -> Result<(), ListShareGroupOffsetsHostError> {
        match effect {
            ListShareGroupOffsetsEffect::Submit {
                operation_id,
                deadline,
                plan,
            } => self.operations[index].install_submission(operation_id, deadline, plan),
            ListShareGroupOffsetsEffect::Complete {
                operation_id,
                terminal,
            } => {
                self.operations[index].install_terminal(operation_id, terminal)?;
                self.publish_terminal(index)
            }
        }
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }

    #[cfg(test)]
    pub(super) fn apply_for_test(
        &mut self,
        operation_id: OperationId,
        input: ListShareGroupOffsetsInput,
        retained_bytes: usize,
    ) -> Result<(), ListShareGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListShareGroupOffsetsHostError::UnknownOperation)?;
        self.operations[index].debit_result_bytes(retained_bytes)?;
        self.apply(operation_id, input)
    }
}
