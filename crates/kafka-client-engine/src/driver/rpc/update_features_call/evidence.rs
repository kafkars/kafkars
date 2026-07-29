//! Exact feature-mutation identity retained across every driver ownership state.

use kafka_client_core::UpdateFeaturesPlan;

#[derive(Debug)]
pub(in crate::driver::rpc) struct UpdateFeaturesEvidence {
    plan: UpdateFeaturesPlan,
    result_limit: usize,
}

impl UpdateFeaturesEvidence {
    pub(in crate::driver::rpc) const fn new(plan: UpdateFeaturesPlan, result_limit: usize) -> Self {
        Self { plan, result_limit }
    }

    pub(in crate::driver::rpc) fn matches(
        &self,
        plan: &UpdateFeaturesPlan,
        result_limit: usize,
    ) -> bool {
        self.plan == *plan && self.result_limit == result_limit
    }

    pub(in crate::driver::rpc) const fn plan(&self) -> &UpdateFeaturesPlan {
        &self.plan
    }

    pub(in crate::driver::rpc) const fn result_limit(&self) -> usize {
        self.result_limit
    }

    pub(in crate::driver::rpc) fn into_parts(self) -> (UpdateFeaturesPlan, usize) {
        (self.plan, self.result_limit)
    }
}
