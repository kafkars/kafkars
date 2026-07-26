//! Test-only observations of exact `OffsetCommit` call and terminal ownership.

use kafka_client_core::{
    AlterConsumerGroupOffsetsInput, AlterConsumerGroupOffsetsPlan, OperationId,
};

use super::super::{AlterConsumerGroupOffsetsHost, AlterConsumerGroupOffsetsHostError};

impl AlterConsumerGroupOffsetsHost {
    pub(in crate::admin::group_offset_alter) fn retain_recovered_call_for_test(
        &mut self,
        plan: AlterConsumerGroupOffsetsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) {
        let recovered = crate::driver::RecoveredGroupOffsetAlterCall::for_test(
            plan,
            request_scratch_limit,
            result_limit,
        );
        super::super::recovered::retain(&mut self.operations[0], Some(recovered));
    }

    pub(in crate::admin::group_offset_alter) fn recovered_call_matches_for_test(
        &self,
        plan: &AlterConsumerGroupOffsetsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.operations[0]
            .recovered_call
            .as_ref()
            .is_some_and(|recovered| {
                recovered.matches_evidence(plan, request_scratch_limit, result_limit)
            })
    }

    pub(in crate::admin::group_offset_alter) fn call_matches_for_test(
        &self,
        plan: &AlterConsumerGroupOffsetsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.operations[0]
            .call
            .as_ref()
            .is_some_and(|call| call.matches_evidence(plan, request_scratch_limit, result_limit))
    }

    pub(in crate::admin::group_offset_alter) fn rejected_submission_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].rejected_submission.is_some()
    }

    pub(in crate::admin::group_offset_alter) fn retain_raw_terminal_for_test(
        &mut self,
        plan: AlterConsumerGroupOffsetsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) {
        self.operations[0].raw_terminal = Some(crate::driver::GroupOffsetAlterTerminal::for_test(
            plan,
            request_scratch_limit,
            result_limit,
        ));
    }

    pub(in crate::admin::group_offset_alter) fn raw_terminal_is_retained_for_test(&self) -> bool {
        self.operations[0].raw_terminal.is_some()
    }

    pub(in crate::admin::group_offset_alter) fn settle_raw_for_test(
        &mut self,
    ) -> Result<(), AlterConsumerGroupOffsetsHostError> {
        self.settle_raw(0)
    }

    pub(in crate::admin::group_offset_alter) fn apply_input_for_test(
        &mut self,
        operation_id: OperationId,
        input: AlterConsumerGroupOffsetsInput,
    ) -> Result<(), AlterConsumerGroupOffsetsHostError> {
        self.apply(operation_id, input)
    }

    pub(in crate::admin::group_offset_alter) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), AlterConsumerGroupOffsetsHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::group_offset_alter) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), AlterConsumerGroupOffsetsHostError> {
        self.publish_terminal(0)
    }
}
