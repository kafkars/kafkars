//! Per-operation plan, terminal, and retained-result ownership transitions.

use kafka_client_core::{
    ListConsumerGroupOffsetsPlan, ListConsumerGroupOffsetsTerminal, OperationId,
};

use super::{
    ListConsumerGroupOffsetsHostError, ListConsumerGroupOffsetsOperation,
    ListConsumerGroupOffsetsSubmission,
};

impl ListConsumerGroupOffsetsOperation {
    pub(super) fn active_plan(
        &self,
    ) -> Result<&ListConsumerGroupOffsetsPlan, ListConsumerGroupOffsetsHostError> {
        self.active_plan
            .as_ref()
            .ok_or(ListConsumerGroupOffsetsHostError::MissingSubmission)
    }

    pub(super) fn install_submission(
        &mut self,
        effect_id: OperationId,
        core_deadline: kafka_client_core::Deadline,
        plan: ListConsumerGroupOffsetsPlan,
    ) -> Result<(), ListConsumerGroupOffsetsHostError> {
        if effect_id != self.operation_id
            || core_deadline != self.deadline.core()
            || plan.group_ids().len() != 1
        {
            return Err(ListConsumerGroupOffsetsHostError::SubmissionMismatch);
        }
        if self.call.is_some() || self.raw_terminal.is_some() || self.terminal.is_some() {
            return Err(ListConsumerGroupOffsetsHostError::InvalidHandoff);
        }
        self.active_plan = Some(plan.clone());
        self.submission = Some(ListConsumerGroupOffsetsSubmission {
            operation_id: self.operation_id,
            deadline: self.deadline,
            plan,
            result_limit: self.remaining_result_bytes,
        });
        self.mark_untouched();
        Ok(())
    }

    pub(super) fn install_terminal(
        &mut self,
        effect_id: OperationId,
        terminal: ListConsumerGroupOffsetsTerminal,
    ) -> Result<(), ListConsumerGroupOffsetsHostError> {
        if effect_id != self.operation_id {
            return Err(ListConsumerGroupOffsetsHostError::SubmissionMismatch);
        }
        if self.call.is_some() || self.raw_terminal.is_some() || self.terminal.is_some() {
            return Err(ListConsumerGroupOffsetsHostError::InvalidHandoff);
        }
        self.active_plan = None;
        self.submission = None;
        self.terminal = Some(terminal);
        Ok(())
    }

    pub(super) fn debit_result_bytes(
        &mut self,
        retained_bytes: usize,
    ) -> Result<(), ListConsumerGroupOffsetsHostError> {
        self.remaining_result_bytes = self
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(ListConsumerGroupOffsetsHostError::ByteAccounting)?;
        Ok(())
    }
}
