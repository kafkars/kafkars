//! Bounded ownership of share-group offset machines and concrete calls.

mod admission;
mod model;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    AlterShareGroupOffsetsEffect, AlterShareGroupOffsetsInput, AlterShareGroupOffsetsMachine,
    AlterShareGroupOffsetsPlan, AlterShareGroupOffsetsTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminAlterShareGroupOffsetsPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{
        AlterShareGroupOffsetsCall, AlterShareGroupOffsetsTerminal as DriverTerminal,
        RecoveredAlterShareGroupOffsetsCall,
    },
};

use super::{AlterShareGroupOffsetsHostError, AlterShareGroupOffsetsObserver};

use model::AlterShareGroupOffsetsHandoff;
pub(crate) use model::{AlterShareGroupOffsetsSubmission, AlterShareGroupOffsetsTurn};

pub(crate) const ALTER_SHARE_GROUP_OFFSETS_CAPACITY: usize = 16;
pub(crate) const ALTER_SHARE_GROUP_OFFSETS_RESULT_BYTES: usize = 2 * 1024 * 1024;
pub(crate) const ALTER_SHARE_GROUP_OFFSETS_RETAINED_BYTES: usize = 6 * 1024 * 1024;

pub(crate) struct AlterShareGroupOffsetsAdmission {
    pub(crate) observer: AlterShareGroupOffsetsObserver,
    pub(crate) fault: Option<AlterShareGroupOffsetsHostError>,
}

struct AlterShareGroupOffsetsOperation {
    operation_id: OperationId,
    machine: AlterShareGroupOffsetsMachine,
    plan: AlterShareGroupOffsetsPlan,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<AlterShareGroupOffsetsSubmission>,
    handoff: AlterShareGroupOffsetsHandoff,
    call: Option<AlterShareGroupOffsetsCall>,
    // Driver-shutdown proof remains live until core accepts the terminal fact.
    recovered_call: Option<RecoveredAlterShareGroupOffsetsCall>,
    raw_terminal: Option<DriverTerminal>,
    terminal: Option<AlterShareGroupOffsetsTerminal>,
}

pub(crate) struct AlterShareGroupOffsetsHost {
    operations: Vec<AlterShareGroupOffsetsOperation>,
    completions:
        CompletionRegistry<AlterShareGroupOffsetsTerminal, AdminAlterShareGroupOffsetsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<AlterShareGroupOffsetsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl AlterShareGroupOffsetsHost {
    pub(crate) fn new(publisher: AdminAlterShareGroupOffsetsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(ALTER_SHARE_GROUP_OFFSETS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                ALTER_SHARE_GROUP_OFFSETS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(ALTER_SHARE_GROUP_OFFSETS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<AlterShareGroupOffsetsTurn, AlterShareGroupOffsetsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(AlterShareGroupOffsetsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(AlterShareGroupOffsetsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, AlterShareGroupOffsetsInput::DeadlineElapsed)?;
            return Ok(AlterShareGroupOffsetsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(AlterShareGroupOffsetsHostError::MissingSubmission)?;
        self.operations[index].handoff = AlterShareGroupOffsetsHandoff::HandedOff;
        Ok(AlterShareGroupOffsetsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: AlterShareGroupOffsetsCall,
    ) -> Result<(), AlterShareGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AlterShareGroupOffsetsHostError::UnknownOperation)?;
        if self.operations[index].handoff != AlterShareGroupOffsetsHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(AlterShareGroupOffsetsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, AlterShareGroupOffsetsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), AlterShareGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AlterShareGroupOffsetsHostError::UnknownOperation)?;
        if self.operations[index].handoff != AlterShareGroupOffsetsHandoff::HandedOff {
            return Err(AlterShareGroupOffsetsHostError::InvalidHandoff);
        }
        self.apply(operation_id, AlterShareGroupOffsetsInput::DriverRejected)
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
        input: AlterShareGroupOffsetsInput,
    ) -> Result<(), AlterShareGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AlterShareGroupOffsetsHostError::UnknownOperation)?;
        let accepted = matches!(&input, AlterShareGroupOffsetsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != AlterShareGroupOffsetsHandoff::HandedOff {
            return Err(AlterShareGroupOffsetsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = AlterShareGroupOffsetsHandoff::Submitted;
        }
        if let Some(AlterShareGroupOffsetsEffect::Complete { terminal, .. }) =
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
