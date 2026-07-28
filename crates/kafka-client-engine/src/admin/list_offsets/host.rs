//! Bounded ownership of accepted Admin `ListOffsets` machines and leader calls.

mod admission;
mod model;
mod response;
mod submission;
mod terminal;

use kafka_client_core::{
    AdminListOffsetsEffect, AdminListOffsetsInput, AdminListOffsetsTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminListOffsetsPublisher,
    completion::{CompletionId, CompletionRegistry},
    driver::AdminListOffsetsCall,
};

use super::{AdminListOffsetsHostError, AdminListOffsetsObserver};
pub(crate) use model::AdminListOffsetsTurn;
use model::{AdminListOffsetsHandoff, AdminListOffsetsOperation};
use submission::AdminListOffsetsSubmission;

pub(crate) const ADMIN_LIST_OFFSETS_CAPACITY: usize = 16;
pub(crate) const ADMIN_LIST_OFFSETS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct AdminListOffsetsAdmission {
    pub(crate) observer: AdminListOffsetsObserver,
    pub(crate) fault: Option<AdminListOffsetsHostError>,
}

pub(crate) struct AdminListOffsetsHost {
    operations: Vec<AdminListOffsetsOperation>,
    completions: CompletionRegistry<AdminListOffsetsTerminal, AdminListOffsetsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<AdminListOffsetsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl AdminListOffsetsHost {
    pub(crate) fn new(publisher: AdminListOffsetsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(ADMIN_LIST_OFFSETS_CAPACITY),
            completions: CompletionRegistry::with_publisher(ADMIN_LIST_OFFSETS_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(ADMIN_LIST_OFFSETS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<AdminListOffsetsTurn, AdminListOffsetsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(AdminListOffsetsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(AdminListOffsetsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, AdminListOffsetsInput::DeadlineElapsed)?;
            return Ok(AdminListOffsetsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(AdminListOffsetsHostError::MissingSubmission)?;
        self.operations[index].handoff = AdminListOffsetsHandoff::HandedOff;
        Ok(AdminListOffsetsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: AdminListOffsetsCall,
    ) -> Result<(), AdminListOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AdminListOffsetsHostError::UnknownOperation)?;
        if self.operations[index].handoff != AdminListOffsetsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
        {
            return Err(AdminListOffsetsHostError::InvalidHandoff);
        }
        let expected_target = self.operations[index]
            .machine
            .current_target()
            .ok_or(AdminListOffsetsHostError::SubmissionMismatch)?;
        if !call.matches_correlation(
            expected_target,
            self.operations[index].machine.read_isolation(),
        ) {
            return Err(AdminListOffsetsHostError::SubmissionMismatch);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, AdminListOffsetsInput::DriverAccepted)
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "rejected handoff returns target ownership through the host boundary"
    )]
    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        target: kafka_client_core::AdminListOffsetTarget,
        read_isolation: kafka_client_core::ReadIsolation,
    ) -> Result<(), AdminListOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AdminListOffsetsHostError::UnknownOperation)?;
        if self.operations[index].handoff != AdminListOffsetsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(AdminListOffsetsHostError::InvalidHandoff);
        }
        if self.operations[index].machine.current_target() != Some(&target)
            || self.operations[index].machine.read_isolation() != read_isolation
        {
            return Err(AdminListOffsetsHostError::SubmissionMismatch);
        }
        self.apply(operation_id, AdminListOffsetsInput::DriverRejected)
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
        input: AdminListOffsetsInput,
    ) -> Result<(), AdminListOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AdminListOffsetsHostError::UnknownOperation)?;
        let accepted = matches!(&input, AdminListOffsetsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != AdminListOffsetsHandoff::HandedOff {
            return Err(AdminListOffsetsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = AdminListOffsetsHandoff::Submitted;
        }
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }

    fn install_effect(
        &mut self,
        index: usize,
        effect: AdminListOffsetsEffect,
    ) -> Result<(), AdminListOffsetsHostError> {
        let operation_id = self.operations[index].operation_id;
        match effect {
            AdminListOffsetsEffect::Submit {
                operation_id: effect_id,
                deadline,
                target,
                read_isolation,
            } => {
                if effect_id != operation_id || deadline != self.operations[index].deadline.core() {
                    return Err(AdminListOffsetsHostError::SubmissionMismatch);
                }
                self.operations[index].submission = Some(AdminListOffsetsSubmission {
                    operation_id,
                    deadline: self.operations[index].deadline,
                    target,
                    read_isolation,
                });
                self.operations[index].handoff = AdminListOffsetsHandoff::Untouched;
                Ok(())
            }
            AdminListOffsetsEffect::Complete {
                operation_id: effect_id,
                terminal,
            } => {
                if effect_id != operation_id {
                    return Err(AdminListOffsetsHostError::SubmissionMismatch);
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
