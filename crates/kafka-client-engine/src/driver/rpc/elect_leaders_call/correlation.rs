//! Exact election attempt evidence retained across linear call states.

use kafka_client_core::ElectLeadersPlan;

/// Exact caller intent and bounded attempt limits carried through every call state.
#[derive(Debug)]
pub(in crate::driver::rpc) struct ElectLeadersCorrelation {
    plan: ElectLeadersPlan,
    request_scratch_limit: usize,
    result_limit: usize,
}

impl ElectLeadersCorrelation {
    pub(in crate::driver::rpc) const fn new(
        plan: ElectLeadersPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            plan,
            request_scratch_limit,
            result_limit,
        }
    }

    pub(in crate::driver::rpc) const fn plan(&self) -> &ElectLeadersPlan {
        &self.plan
    }

    pub(in crate::driver::rpc) const fn result_limit(&self) -> usize {
        self.result_limit
    }

    pub(in crate::driver::rpc) fn matches(
        &self,
        plan: &ElectLeadersPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        &self.plan == plan
            && self.request_scratch_limit == request_scratch_limit
            && self.result_limit == result_limit
    }

    pub(in crate::driver::rpc) fn into_parts(self) -> (ElectLeadersPlan, usize, usize) {
        (self.plan, self.request_scratch_limit, self.result_limit)
    }
}
