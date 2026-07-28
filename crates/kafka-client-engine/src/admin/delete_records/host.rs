//! Bounded ownership of accepted Admin `DeleteRecords` machines and leader calls.

mod admission;
mod response;
mod terminal;

use kafka_client_core::{
    DeleteRecordsEffect, DeleteRecordsInput, DeleteRecordsMachine, DeleteRecordsTarget,
    DeleteRecordsTerminal, Moment, OperationId,
};

use crate::{
    admin::DeleteRecordsPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{DeleteRecordsCall, DeleteRecordsRawTerminal, RecoveredDeleteRecordsCall},
};

use super::{DeleteRecordsHostError, DeleteRecordsObserver};

pub(crate) const DELETE_RECORDS_CAPACITY: usize = 16;
pub(crate) const DELETE_RECORDS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct DeleteRecordsAdmission {
    pub(crate) observer: DeleteRecordsObserver,
    pub(crate) fault: Option<DeleteRecordsHostError>,
}

/// One exact target ready for the engine's driver-admission stage.
pub(crate) struct DeleteRecordsSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    target: DeleteRecordsTarget,
}

impl DeleteRecordsSubmission {
    pub(crate) fn into_parts(self) -> (OperationId, OperationDeadline, DeleteRecordsTarget) {
        (self.operation_id, self.deadline, self.target)
    }
}

pub(crate) enum DeleteRecordsTurn {
    Idle,
    Progress,
    Submit(DeleteRecordsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeleteRecordsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct DeleteRecordsOperation {
    operation_id: OperationId,
    machine: DeleteRecordsMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    submission: Option<DeleteRecordsSubmission>,
    handoff: DeleteRecordsHandoff,
    call: Option<DeleteRecordsCall>,
    // Driver-shutdown proof remains live until core accepts the terminal fact.
    recovered_call: Option<RecoveredDeleteRecordsCall>,
    raw_terminal: Option<DeleteRecordsRawTerminal>,
    terminal: Option<DeleteRecordsTerminal>,
}

pub(crate) struct DeleteRecordsHost {
    operations: Vec<DeleteRecordsOperation>,
    completions: CompletionRegistry<DeleteRecordsTerminal, DeleteRecordsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<DeleteRecordsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl DeleteRecordsHost {
    pub(crate) fn new(publisher: DeleteRecordsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DELETE_RECORDS_CAPACITY),
            completions: CompletionRegistry::with_publisher(DELETE_RECORDS_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DELETE_RECORDS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<DeleteRecordsTurn, DeleteRecordsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(DeleteRecordsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DeleteRecordsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, DeleteRecordsInput::DeadlineElapsed)?;
            return Ok(DeleteRecordsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DeleteRecordsHostError::MissingSubmission)?;
        self.operations[index].handoff = DeleteRecordsHandoff::HandedOff;
        Ok(DeleteRecordsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: DeleteRecordsCall,
    ) -> Result<(), DeleteRecordsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DeleteRecordsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DeleteRecordsHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(DeleteRecordsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, DeleteRecordsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), DeleteRecordsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DeleteRecordsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DeleteRecordsHandoff::HandedOff {
            return Err(DeleteRecordsHostError::InvalidHandoff);
        }
        self.apply(operation_id, DeleteRecordsInput::DriverRejected)
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
        input: DeleteRecordsInput,
    ) -> Result<(), DeleteRecordsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DeleteRecordsHostError::UnknownOperation)?;
        let accepted = matches!(&input, DeleteRecordsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != DeleteRecordsHandoff::HandedOff {
            return Err(DeleteRecordsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = DeleteRecordsHandoff::Submitted;
        }
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }

    fn install_effect(
        &mut self,
        index: usize,
        effect: DeleteRecordsEffect,
    ) -> Result<(), DeleteRecordsHostError> {
        let operation_id = self.operations[index].operation_id;
        match effect {
            DeleteRecordsEffect::Submit {
                operation_id: effect_id,
                deadline,
                target,
            } => {
                if effect_id != operation_id || deadline != self.operations[index].deadline.core() {
                    return Err(DeleteRecordsHostError::SubmissionMismatch);
                }
                self.operations[index].submission = Some(DeleteRecordsSubmission {
                    operation_id,
                    deadline: self.operations[index].deadline,
                    target,
                });
                self.operations[index].handoff = DeleteRecordsHandoff::Untouched;
                Ok(())
            }
            DeleteRecordsEffect::Complete {
                operation_id: effect_id,
                terminal,
            } => {
                if effect_id != operation_id {
                    return Err(DeleteRecordsHostError::SubmissionMismatch);
                }
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
        }
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }
}
