//! Test-only observations of group-offset deletion ownership.

use super::super::{DeleteConsumerGroupOffsetsHost, DeleteConsumerGroupOffsetsHostError};

impl DeleteConsumerGroupOffsetsHost {
    pub(in crate::admin::group_offset_delete) fn retain_recovered_call_for_test(&mut self) {
        let plan = self.operations[0].response_plan.clone();
        let result_limit = self.operations[0].result_limit;
        self.operations[0].recovered_call = Some(
            crate::driver::RecoveredGroupOffsetDeleteCall::for_test(plan, result_limit),
        );
    }

    pub(in crate::admin::group_offset_delete) fn retain_mismatched_recovered_call_for_test(
        &mut self,
    ) {
        let plan = self.operations[0].response_plan.clone();
        let result_limit = self.operations[0].result_limit.saturating_sub(1);
        self.operations[0].recovered_call = Some(
            crate::driver::RecoveredGroupOffsetDeleteCall::for_test(plan, result_limit),
        );
    }

    pub(in crate::admin::group_offset_delete) fn recovered_call_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    pub(in crate::admin::group_offset_delete) fn call_ownership_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].call.is_some()
    }

    pub(in crate::admin::group_offset_delete) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), DeleteConsumerGroupOffsetsHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::group_offset_delete) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), DeleteConsumerGroupOffsetsHostError> {
        self.publish_terminal(0)
    }
}
