//! Exact ACL-deletion intent and retained bounds across linear call states.

use kafka_client_core::DeleteAclsPlan;

#[derive(Debug)]
pub(in crate::driver::rpc) struct DeleteAclsEvidence {
    plan: DeleteAclsPlan,
    request_limit: usize,
    nested_count_capacity: usize,
    result_capacity: usize,
    outcome_capacity: usize,
}

impl DeleteAclsEvidence {
    pub(in crate::driver::rpc) const fn new(
        plan: DeleteAclsPlan,
        request_limit: usize,
        nested_count_capacity: usize,
        result_capacity: usize,
        outcome_capacity: usize,
    ) -> Self {
        Self {
            plan,
            request_limit,
            nested_count_capacity,
            result_capacity,
            outcome_capacity,
        }
    }

    pub(in crate::driver::rpc) const fn plan(&self) -> &DeleteAclsPlan {
        &self.plan
    }

    pub(in crate::driver::rpc) const fn request_limit(&self) -> usize {
        self.request_limit
    }

    pub(in crate::driver::rpc) fn matches(
        &self,
        plan: &DeleteAclsPlan,
        request_limit: usize,
        nested_count_capacity: usize,
        result_capacity: usize,
        outcome_capacity: usize,
    ) -> bool {
        self.plan == *plan
            && self.request_limit == request_limit
            && self.nested_count_capacity == nested_count_capacity
            && self.result_capacity == result_capacity
            && self.outcome_capacity == outcome_capacity
    }

    pub(in crate::driver::rpc) fn into_parts(self) -> (DeleteAclsPlan, usize, usize, usize, usize) {
        (
            self.plan,
            self.request_limit,
            self.nested_count_capacity,
            self.result_capacity,
            self.outcome_capacity,
        )
    }
}
