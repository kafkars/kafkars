//! Focused observation and raw-evidence injection for ownership tests.

use kafka_client_core::AdminDescribeConsumerGroupsCallKind;

use crate::driver::DescribeConsumerGroupsTerminal;

use super::super::{DescribeConsumerGroupsHost, DescribeConsumerGroupsHostError};

impl DescribeConsumerGroupsHost {
    pub(in crate::admin::describe_consumer_groups) fn route_plan_for_test(&self) -> &[String] {
        self.operations
            .first()
            .map(|operation| operation.route_plan.groups())
            .unwrap_or(&[])
    }

    pub(in crate::admin::describe_consumer_groups) fn recovered_matches_for_test(
        &self,
        group_id: &str,
        include_authorized_operations: bool,
        call_kind: AdminDescribeConsumerGroupsCallKind,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.operations
            .first()
            .and_then(|operation| operation.recovered_call.as_ref())
            .is_some_and(|recovered| {
                recovered.matches_evidence(
                    group_id,
                    include_authorized_operations,
                    call_kind,
                    request_scratch_limit,
                    result_limit,
                )
            })
    }

    pub(in crate::admin::describe_consumer_groups) fn replace_call_with_raw_for_test(
        &mut self,
        group_id: String,
        include_authorized_operations: bool,
        call_kind: AdminDescribeConsumerGroupsCallKind,
        request_scratch_limit: usize,
        result_limit: usize,
    ) {
        let operation = self
            .operations
            .first_mut()
            .unwrap_or_else(|| panic!("operation expected"));
        let call = operation
            .call
            .take()
            .unwrap_or_else(|| panic!("accepted call expected"));
        call.recover_after_driver_shutdown()
            .unwrap_or_else(|| panic!("recovered call expected"))
            .seal();
        operation.raw_terminal = Some(DescribeConsumerGroupsTerminal::for_test(
            group_id,
            include_authorized_operations,
            call_kind,
            request_scratch_limit,
            result_limit,
        ));
    }

    pub(in crate::admin::describe_consumer_groups) fn settle_raw_for_test(
        &mut self,
    ) -> Result<(), DescribeConsumerGroupsHostError> {
        self.settle_raw(0)
    }

    pub(in crate::admin::describe_consumer_groups) fn raw_is_retained_for_test(&self) -> bool {
        self.operations
            .first()
            .is_some_and(|operation| operation.raw_terminal.is_some())
    }

    pub(in crate::admin::describe_consumer_groups) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), DescribeConsumerGroupsHostError> {
        self.publish_terminal(0)
    }
}
