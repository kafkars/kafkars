//! One accepted API-90 operation and its current singleton handoff projection.

use kafka_client_core::{
    ListShareGroupOffsetsMachine, ListShareGroupOffsetsPlan, ListShareGroupOffsetsTerminal,
    OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    driver::{ListShareGroupOffsetsCall, ListShareGroupOffsetsTerminal as DriverTerminal},
};

use super::{
    ListShareGroupOffsetsHandoff, ListShareGroupOffsetsHostError, ListShareGroupOffsetsSubmission,
};

pub(super) struct ListShareGroupOffsetsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: ListShareGroupOffsetsMachine,
    pub(super) active_plan: Option<ListShareGroupOffsetsPlan>,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) remaining_result_bytes: usize,
    pub(super) submission: Option<ListShareGroupOffsetsSubmission>,
    pub(super) handoff: ListShareGroupOffsetsHandoff,
    pub(super) call: Option<ListShareGroupOffsetsCall>,
    pub(super) raw_terminal: Option<DriverTerminal>,
    pub(super) terminal: Option<ListShareGroupOffsetsTerminal>,
}

impl ListShareGroupOffsetsOperation {
    pub(super) fn active_plan(
        &self,
    ) -> Result<&ListShareGroupOffsetsPlan, ListShareGroupOffsetsHostError> {
        self.active_plan
            .as_ref()
            .ok_or(ListShareGroupOffsetsHostError::MissingSubmission)
    }

    pub(super) fn install_submission(
        &mut self,
        effect_id: OperationId,
        core_deadline: kafka_client_core::Deadline,
        plan: ListShareGroupOffsetsPlan,
    ) -> Result<(), ListShareGroupOffsetsHostError> {
        if effect_id != self.operation_id
            || core_deadline != self.deadline.core()
            || plan.queries().len() != 1
        {
            return Err(ListShareGroupOffsetsHostError::SubmissionMismatch);
        }
        if self.call.is_some() || self.raw_terminal.is_some() || self.terminal.is_some() {
            return Err(ListShareGroupOffsetsHostError::InvalidHandoff);
        }
        self.active_plan = Some(plan.clone());
        self.submission = Some(ListShareGroupOffsetsSubmission {
            operation_id: self.operation_id,
            deadline: self.deadline,
            plan,
            result_limit: self.remaining_result_bytes,
        });
        self.handoff = ListShareGroupOffsetsHandoff::Untouched;
        Ok(())
    }

    pub(super) fn install_terminal(
        &mut self,
        effect_id: OperationId,
        terminal: ListShareGroupOffsetsTerminal,
    ) -> Result<(), ListShareGroupOffsetsHostError> {
        if effect_id != self.operation_id {
            return Err(ListShareGroupOffsetsHostError::SubmissionMismatch);
        }
        if self.call.is_some() || self.raw_terminal.is_some() || self.terminal.is_some() {
            return Err(ListShareGroupOffsetsHostError::InvalidHandoff);
        }
        self.active_plan = None;
        self.submission = None;
        self.terminal = Some(terminal);
        Ok(())
    }

    pub(super) fn debit_result_bytes(
        &mut self,
        retained_bytes: usize,
    ) -> Result<(), ListShareGroupOffsetsHostError> {
        self.remaining_result_bytes = self
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(ListShareGroupOffsetsHostError::ByteAccounting)?;
        Ok(())
    }
}
