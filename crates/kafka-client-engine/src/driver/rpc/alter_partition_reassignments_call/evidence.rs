//! Exact submission identity retained across every driver ownership state.

use kafka_client_core::AlterPartitionReassignmentsPlan;

#[derive(Debug)]
pub(in crate::driver::rpc) struct AlterPartitionReassignmentsEvidence {
    plan: AlterPartitionReassignmentsPlan,
    request_scratch_limit: usize,
    result_limit: usize,
}

impl AlterPartitionReassignmentsEvidence {
    pub(in crate::driver::rpc) const fn new(
        plan: AlterPartitionReassignmentsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            plan,
            request_scratch_limit,
            result_limit,
        }
    }

    pub(in crate::driver::rpc) fn matches(
        &self,
        plan: &AlterPartitionReassignmentsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.plan == *plan
            && self.request_scratch_limit == request_scratch_limit
            && self.result_limit == result_limit
    }

    pub(in crate::driver::rpc) const fn plan(&self) -> &AlterPartitionReassignmentsPlan {
        &self.plan
    }

    pub(in crate::driver::rpc) const fn result_limit(&self) -> usize {
        self.result_limit
    }

    pub(in crate::driver::rpc) fn into_parts(
        self,
    ) -> (AlterPartitionReassignmentsPlan, usize, usize) {
        (self.plan, self.request_scratch_limit, self.result_limit)
    }
}
