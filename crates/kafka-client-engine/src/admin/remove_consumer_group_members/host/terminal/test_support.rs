//! Narrow ownership probes for member-removal host tests.

use kafka_client_core::{
    OperationId, RemoveConsumerGroupMembersInput, RemoveConsumerGroupMembersPlan,
};

use super::super::{RemoveConsumerGroupMembersHost, RemoveConsumerGroupMembersHostError};

impl RemoveConsumerGroupMembersHost {
    pub(in crate::admin::remove_consumer_group_members) fn call_matches_for_test(
        &self,
        plan: &RemoveConsumerGroupMembersPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.operations[0]
            .call
            .as_ref()
            .is_some_and(|call| call.matches(plan, request_scratch_limit, result_limit))
    }

    pub(in crate::admin::remove_consumer_group_members) fn recovered_matches_for_test(
        &self,
        plan: &RemoveConsumerGroupMembersPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.operations[0]
            .recovered_call
            .as_ref()
            .is_some_and(|call| call.matches(plan, request_scratch_limit, result_limit))
    }

    pub(in crate::admin::remove_consumer_group_members) fn rejected_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].rejected_submission.is_some()
    }

    pub(in crate::admin::remove_consumer_group_members) fn apply_input_for_test(
        &mut self,
        operation_id: OperationId,
        input: RemoveConsumerGroupMembersInput,
    ) -> Result<(), RemoveConsumerGroupMembersHostError> {
        self.apply(operation_id, input)
    }

    pub(in crate::admin::remove_consumer_group_members) fn retain_raw_for_test(
        &mut self,
        raw: crate::driver::RemoveConsumerGroupMembersTerminal,
    ) {
        self.operations[0].raw_terminal = Some(raw);
    }

    pub(in crate::admin::remove_consumer_group_members) fn settle_raw_for_test(
        &mut self,
    ) -> Result<(), RemoveConsumerGroupMembersHostError> {
        self.settle_raw(0)
    }

    pub(in crate::admin::remove_consumer_group_members) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), RemoveConsumerGroupMembersHostError> {
        self.publish_terminal(0)
    }
}
