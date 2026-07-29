//! Bounded ownership of share-group offset machines and concrete calls.

mod admission;
mod model;
#[cfg(test)]
mod ownership_test;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    DeleteShareGroupOffsetsEffect, DeleteShareGroupOffsetsInput, DeleteShareGroupOffsetsMachine,
    DeleteShareGroupOffsetsPlan, DeleteShareGroupOffsetsTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminDeleteShareGroupOffsetsPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{
        DeleteShareGroupOffsetsCall, DeleteShareGroupOffsetsTerminal as DriverTerminal,
        RecoveredDeleteShareGroupOffsetsCall,
    },
};

use super::{DeleteShareGroupOffsetsHostError, DeleteShareGroupOffsetsObserver};

use model::DeleteShareGroupOffsetsHandoff;
pub(crate) use model::{DeleteShareGroupOffsetsSubmission, DeleteShareGroupOffsetsTurn};

pub(crate) const DELETE_SHARE_GROUP_OFFSETS_CAPACITY: usize = 16;
pub(crate) const DELETE_SHARE_GROUP_OFFSETS_RESULT_BYTES: usize = 2 * 1024 * 1024;
pub(crate) const DELETE_SHARE_GROUP_OFFSETS_RETAINED_BYTES: usize = 6 * 1024 * 1024;

pub(crate) struct DeleteShareGroupOffsetsAdmission {
    pub(crate) observer: DeleteShareGroupOffsetsObserver,
    pub(crate) fault: Option<DeleteShareGroupOffsetsHostError>,
}

struct DeleteShareGroupOffsetsOperation {
    operation_id: OperationId,
    machine: DeleteShareGroupOffsetsMachine,
    plan: DeleteShareGroupOffsetsPlan,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<DeleteShareGroupOffsetsSubmission>,
    handoff: DeleteShareGroupOffsetsHandoff,
    call: Option<DeleteShareGroupOffsetsCall>,
    recovered_call: Option<RecoveredDeleteShareGroupOffsetsCall>,
    raw_terminal: Option<DriverTerminal>,
    terminal: Option<DeleteShareGroupOffsetsTerminal>,
}

pub(crate) struct DeleteShareGroupOffsetsHost {
    operations: Vec<DeleteShareGroupOffsetsOperation>,
    completions:
        CompletionRegistry<DeleteShareGroupOffsetsTerminal, AdminDeleteShareGroupOffsetsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<DeleteShareGroupOffsetsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl DeleteShareGroupOffsetsHost {
    pub(crate) fn new(publisher: AdminDeleteShareGroupOffsetsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DELETE_SHARE_GROUP_OFFSETS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                DELETE_SHARE_GROUP_OFFSETS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DELETE_SHARE_GROUP_OFFSETS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<DeleteShareGroupOffsetsTurn, DeleteShareGroupOffsetsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(DeleteShareGroupOffsetsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DeleteShareGroupOffsetsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, DeleteShareGroupOffsetsInput::DeadlineElapsed)?;
            return Ok(DeleteShareGroupOffsetsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DeleteShareGroupOffsetsHostError::MissingSubmission)?;
        self.operations[index].handoff = DeleteShareGroupOffsetsHandoff::HandedOff;
        Ok(DeleteShareGroupOffsetsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: DeleteShareGroupOffsetsCall,
    ) -> Result<(), DeleteShareGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DeleteShareGroupOffsetsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DeleteShareGroupOffsetsHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(DeleteShareGroupOffsetsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, DeleteShareGroupOffsetsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), DeleteShareGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DeleteShareGroupOffsetsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DeleteShareGroupOffsetsHandoff::HandedOff {
            return Err(DeleteShareGroupOffsetsHostError::InvalidHandoff);
        }
        self.apply(operation_id, DeleteShareGroupOffsetsInput::DriverRejected)
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
        input: DeleteShareGroupOffsetsInput,
    ) -> Result<(), DeleteShareGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DeleteShareGroupOffsetsHostError::UnknownOperation)?;
        let accepted = matches!(&input, DeleteShareGroupOffsetsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != DeleteShareGroupOffsetsHandoff::HandedOff {
            return Err(DeleteShareGroupOffsetsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = DeleteShareGroupOffsetsHandoff::Submitted;
        }
        if let Some(DeleteShareGroupOffsetsEffect::Complete { terminal, .. }) =
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
