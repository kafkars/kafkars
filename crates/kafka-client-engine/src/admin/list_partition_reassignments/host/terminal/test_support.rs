//! Test-only observations of list-reassignment retained ownership.

use kafka_client_core::{
    ListPartitionReassignmentsInput, ListPartitionReassignmentsPlan, OperationId,
};

use super::super::{ListPartitionReassignmentsHost, ListPartitionReassignmentsHostError};

impl ListPartitionReassignmentsHost {
    pub(in crate::admin::list_partition_reassignments) const fn retained_bytes_for_test(
        &self,
    ) -> usize {
        self.retained_bytes
    }

    pub(in crate::admin::list_partition_reassignments) fn retain_recovered_call_for_test(
        &mut self,
    ) {
        let plan = self.operations[0].plan.clone();
        let result_limit = self.operations[0].result_limit;
        self.operations[0].recovered_call = Some(
            crate::driver::RecoveredListPartitionReassignmentsCall::for_test(plan, result_limit),
        );
    }

    pub(in crate::admin::list_partition_reassignments) fn recovered_call_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    pub(in crate::admin::list_partition_reassignments) fn call_matches_for_test(
        &self,
        plan: &ListPartitionReassignmentsPlan,
        result_limit: usize,
    ) -> bool {
        self.operations[0]
            .call
            .as_ref()
            .is_some_and(|call| call.matches(plan, result_limit))
    }

    pub(in crate::admin::list_partition_reassignments) fn recovered_matches_for_test(
        &self,
        plan: &ListPartitionReassignmentsPlan,
        result_limit: usize,
    ) -> bool {
        self.operations[0]
            .recovered_call
            .as_ref()
            .is_some_and(|recovered| recovered.matches(plan, result_limit))
    }

    pub(in crate::admin::list_partition_reassignments) fn rejected_submission_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].rejected_submission.is_some()
    }

    pub(in crate::admin::list_partition_reassignments) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), ListPartitionReassignmentsHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::list_partition_reassignments) fn retain_raw_terminal_for_test(
        &mut self,
        raw: crate::driver::ListPartitionReassignmentsRawTerminal,
    ) {
        self.operations[0].raw_terminal = Some(raw);
    }

    pub(in crate::admin::list_partition_reassignments) fn settle_raw_for_test(
        &mut self,
    ) -> Result<(), ListPartitionReassignmentsHostError> {
        self.settle_raw(0)
    }

    pub(in crate::admin::list_partition_reassignments) fn apply_input_for_test(
        &mut self,
        operation_id: OperationId,
        input: ListPartitionReassignmentsInput,
    ) -> Result<(), ListPartitionReassignmentsHostError> {
        self.apply(operation_id, input)
    }

    pub(in crate::admin::list_partition_reassignments) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), ListPartitionReassignmentsHostError> {
        self.publish_terminal(0)
    }
}
