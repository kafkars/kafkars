//! One accepted API-90 operation and its current singleton handoff projection.

use kafka_client_core::{
    ListShareGroupOffsetsMachine, ListShareGroupOffsetsPlan, ListShareGroupOffsetsTerminal,
    OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    driver::{
        ListShareGroupOffsetsCall, ListShareGroupOffsetsTerminal as DriverTerminal,
        RecoveredListShareGroupOffsetsCall,
    },
};

#[cfg(test)]
use super::ListShareGroupOffsetsHost;
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
    pub(super) recovered_call: Option<RecoveredListShareGroupOffsetsCall>,
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
        if self.call.is_some()
            || self.recovered_call.is_some()
            || self.raw_terminal.is_some()
            || self.terminal.is_some()
        {
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
        if self.call.is_some()
            || self.recovered_call.is_some()
            || self.raw_terminal.is_some()
            || self.terminal.is_some()
        {
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

#[cfg(test)]
impl ListShareGroupOffsetsHost {
    pub(in crate::admin::list_share_group_offsets) fn retain_recovered_call_for_test(&mut self) {
        self.operations[0].recovered_call = Some(RecoveredListShareGroupOffsetsCall::for_test());
    }

    pub(in crate::admin::list_share_group_offsets) fn recovered_call_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    pub(in crate::admin::list_share_group_offsets) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), ListShareGroupOffsetsHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::list_share_group_offsets) fn install_live_call_for_test(
        &mut self,
        operation_id: OperationId,
    ) -> crate::driver::DriverOwner {
        let index = self
            .operation_index(operation_id)
            .unwrap_or_else(|| panic!("known API-90 operation"));
        let driver = crate::driver::DriverOwner::build(&crate::EngineConfig::new(vec![
            "127.0.0.1:1".to_owned(),
        ]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
        let call = ListShareGroupOffsetsCall::submit(
            &driver,
            self.operations[index]
                .active_plan()
                .unwrap_or_else(|error| panic!("active plan: {error}")),
            self.operations[index].deadline.transport(),
        )
        .unwrap_or_else(|error| panic!("accepted call: {error}"));
        self.accept_call(operation_id, call)
            .unwrap_or_else(|error| panic!("host acceptance: {error}"));
        driver
    }
}
