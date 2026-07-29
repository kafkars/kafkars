//! Per-operation plan, terminal, and retained-result ownership transitions.

#[cfg(test)]
mod test_support;

use kafka_client_core::{
    ListConsumerGroupOffsetsInput, ListConsumerGroupOffsetsPlan, ListConsumerGroupOffsetsTerminal,
    OperationId,
};

use crate::driver::GroupOffsetsCall;

use super::{
    ListConsumerGroupOffsetsHandoff, ListConsumerGroupOffsetsHost,
    ListConsumerGroupOffsetsHostError, ListConsumerGroupOffsetsOperation,
    ListConsumerGroupOffsetsSubmission,
};

impl ListConsumerGroupOffsetsHost {
    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: GroupOffsetsCall,
    ) -> Result<(), ListConsumerGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListConsumerGroupOffsetsHostError::UnknownOperation)?;
        let operation = &self.operations[index];
        if operation.handoff != ListConsumerGroupOffsetsHandoff::HandedOff
            || operation.call.is_some()
            || operation.recovered_call.is_some()
            || operation.raw_terminal.is_some()
            || operation.rejected_submission.is_some()
        {
            return Err(ListConsumerGroupOffsetsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        let operation = &self.operations[index];
        if !operation
            .call
            .as_ref()
            .is_some_and(|call| operation.matches_call(call))
        {
            return Err(ListConsumerGroupOffsetsHostError::SubmissionMismatch);
        }
        self.apply(operation_id, ListConsumerGroupOffsetsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        plan: ListConsumerGroupOffsetsPlan,
        result_limit: usize,
    ) -> Result<(), ListConsumerGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListConsumerGroupOffsetsHostError::UnknownOperation)?;
        let operation = &self.operations[index];
        if operation.handoff != ListConsumerGroupOffsetsHandoff::HandedOff
            || operation.call.is_some()
            || operation.recovered_call.is_some()
            || operation.raw_terminal.is_some()
            || operation.rejected_submission.is_some()
        {
            return Err(ListConsumerGroupOffsetsHostError::InvalidHandoff);
        }
        let matches = operation.matches_evidence(&plan, result_limit);
        self.operations[index].rejected_submission = Some((plan, result_limit));
        if !matches {
            return Err(ListConsumerGroupOffsetsHostError::SubmissionMismatch);
        }
        let transition = self.operations[index]
            .machine
            .apply(ListConsumerGroupOffsetsInput::DriverRejected)?;
        let effect = transition
            .into_effect()
            .ok_or(ListConsumerGroupOffsetsHostError::MissingTerminal)?;
        drop(self.operations[index].rejected_submission.take());
        self.operations[index].active_plan = None;
        self.install_effect(index, effect)
    }
}

impl ListConsumerGroupOffsetsOperation {
    fn matches_evidence(&self, plan: &ListConsumerGroupOffsetsPlan, result_limit: usize) -> bool {
        self.active_plan.as_ref() == Some(plan) && self.remaining_result_bytes == result_limit
    }

    fn matches_call(&self, call: &GroupOffsetsCall) -> bool {
        self.active_plan
            .as_ref()
            .is_some_and(|plan| call.matches_evidence(plan, self.remaining_result_bytes))
    }

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
        if self.call.is_some()
            || self.recovered_call.is_some()
            || self.raw_terminal.is_some()
            || self.rejected_submission.is_some()
            || self.terminal.is_some()
        {
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
        if self.call.is_some()
            || self.recovered_call.is_some()
            || self.raw_terminal.is_some()
            || self.rejected_submission.is_some()
            || self.terminal.is_some()
        {
            return Err(ListConsumerGroupOffsetsHostError::InvalidHandoff);
        }
        self.active_plan = None;
        self.submission = None;
        self.terminal = Some(terminal);
        Ok(())
    }

    #[cfg(test)]
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
