//! Test-only observations of recovered transaction-correlation ownership.

use kafka_client_core::AbortPartitionTransactionPlan;

use super::super::{AbortPartitionTransactionHost, AbortPartitionTransactionHostError};

impl AbortPartitionTransactionHost {
    pub(in crate::admin::abort_partition_transaction) fn retain_recovered_call_for_test(
        &mut self,
        plan: AbortPartitionTransactionPlan,
    ) {
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredAbortPartitionTransactionCall::for_test(plan));
    }

    pub(in crate::admin::abort_partition_transaction) fn recovered_plan_matches_for_test(
        &self,
        expected: &AbortPartitionTransactionPlan,
    ) -> bool {
        self.operations[0]
            .recovered_call
            .as_ref()
            .is_some_and(|recovered| recovered.matches_plan_for_test(expected))
    }

    pub(in crate::admin::abort_partition_transaction) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), AbortPartitionTransactionHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::abort_partition_transaction) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), AbortPartitionTransactionHostError> {
        self.publish_terminal(0)
    }
}
