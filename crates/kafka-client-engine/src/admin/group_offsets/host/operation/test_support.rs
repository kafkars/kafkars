//! Test-only exact-call installation, recovery, and ownership observations.

use kafka_client_core::OperationId;

use super::super::{ListConsumerGroupOffsetsHost, ListConsumerGroupOffsetsHostError};

impl ListConsumerGroupOffsetsHost {
    pub(in crate::admin::group_offsets) fn retain_recovered_call_for_test(&mut self) {
        let plan = self.operations[0]
            .active_plan()
            .unwrap_or_else(|error| panic!("active plan: {error}"))
            .clone();
        let result_limit = self.operations[0].remaining_result_bytes;
        self.operations[0].recovered_call = Some(
            crate::driver::RecoveredGroupOffsetsCall::for_test(plan, result_limit),
        );
    }

    pub(in crate::admin::group_offsets) fn retain_mismatched_recovered_call_for_test(&mut self) {
        let plan = self.operations[0]
            .active_plan()
            .unwrap_or_else(|error| panic!("active plan: {error}"))
            .clone();
        let result_limit = self.operations[0].remaining_result_bytes.saturating_sub(1);
        self.operations[0].recovered_call = Some(
            crate::driver::RecoveredGroupOffsetsCall::for_test(plan, result_limit),
        );
    }

    pub(in crate::admin::group_offsets) fn recovered_call_is_retained_for_test(&self) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    pub(in crate::admin::group_offsets) fn rejected_submission_is_retained_for_test(&self) -> bool {
        self.operations[0].rejected_submission.is_some()
    }

    pub(in crate::admin::group_offsets) fn call_is_retained_for_test(&self) -> bool {
        self.operations[0].call.is_some()
    }

    pub(in crate::admin::group_offsets) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), ListConsumerGroupOffsetsHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::group_offsets) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), ListConsumerGroupOffsetsHostError> {
        self.publish_terminal(0)
    }

    pub(in crate::admin::group_offsets) fn install_live_call_for_test(
        &mut self,
        operation_id: OperationId,
    ) -> crate::driver::DriverOwner {
        let index = self
            .operation_index(operation_id)
            .unwrap_or_else(|| panic!("known group-offset operation"));
        let driver = crate::driver::DriverOwner::build(&crate::EngineConfig::new(vec![
            "127.0.0.1:1".to_owned(),
        ]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
        let plan = self.operations[index]
            .active_plan()
            .unwrap_or_else(|error| panic!("active plan: {error}"))
            .clone();
        let result_limit = self.operations[index].remaining_result_bytes;
        let call = crate::driver::GroupOffsetsCall::submit(
            &driver,
            plan,
            result_limit,
            self.operations[index].deadline.transport(),
        )
        .unwrap_or_else(|_failure| panic!("accepted call"));
        self.accept_call(operation_id, call)
            .unwrap_or_else(|error| panic!("host acceptance: {error}"));
        driver
    }
}
