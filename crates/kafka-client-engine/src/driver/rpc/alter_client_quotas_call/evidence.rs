//! Exact caller intent and retained-capacity evidence for one destructive attempt.

use kafka_client_core::AlterClientQuotasPlan;

/// Correlation facts that must survive every accepted and rejected driver path.
#[derive(Debug)]
pub(in crate::driver::rpc) struct AlterClientQuotasEvidence {
    plan: AlterClientQuotasPlan,
    retained_limit: usize,
}

impl AlterClientQuotasEvidence {
    pub(in crate::driver::rpc) const fn new(
        plan: AlterClientQuotasPlan,
        retained_limit: usize,
    ) -> Self {
        Self {
            plan,
            retained_limit,
        }
    }

    pub(in crate::driver::rpc) const fn plan(&self) -> &AlterClientQuotasPlan {
        &self.plan
    }

    pub(in crate::driver::rpc) const fn retained_limit(&self) -> usize {
        self.retained_limit
    }

    pub(in crate::driver::rpc) fn matches(
        &self,
        expected_plan: &AlterClientQuotasPlan,
        expected_retained_limit: usize,
    ) -> bool {
        self.plan == *expected_plan && self.retained_limit == expected_retained_limit
    }

    pub(in crate::driver::rpc) fn into_parts(self) -> (AlterClientQuotasPlan, usize) {
        (self.plan, self.retained_limit)
    }
}
