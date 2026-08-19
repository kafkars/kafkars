//! Narrow ownership probes for `ListConsumerGroups` host tests.

use kafka_client_core::{AdminListConsumerGroupsInput, OperationId};

use super::super::{ListConsumerGroupsHost, ListConsumerGroupsHostError};

impl ListConsumerGroupsHost {
    pub(in crate::admin::list_consumer_groups) fn apply_input_for_test(
        &mut self,
        operation_id: OperationId,
        input: AdminListConsumerGroupsInput,
    ) -> Result<(), ListConsumerGroupsHostError> {
        self.apply(operation_id, input)
    }

    pub(in crate::admin::list_consumer_groups) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), ListConsumerGroupsHostError> {
        self.publish_terminal(0)
    }

    pub(in crate::admin::list_consumer_groups) fn recovered_call_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0]
            .call
            .as_ref()
            .is_some_and(crate::driver::ListConsumerGroupsCall::is_recovered)
    }

    pub(in crate::admin::list_consumer_groups) fn rejected_submission_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].rejected_submission.is_some()
    }
}
