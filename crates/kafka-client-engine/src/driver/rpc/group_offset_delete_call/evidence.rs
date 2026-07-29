//! Exact offset-deletion identity retained across every driver ownership state.

use kafka_client_core::DeleteConsumerGroupOffsetsPlan;

#[derive(Debug)]
pub(in crate::driver::rpc) struct GroupOffsetDeleteEvidence {
    plan: DeleteConsumerGroupOffsetsPlan,
    result_limit: usize,
}

impl GroupOffsetDeleteEvidence {
    pub(in crate::driver::rpc) const fn new(
        plan: DeleteConsumerGroupOffsetsPlan,
        result_limit: usize,
    ) -> Self {
        Self { plan, result_limit }
    }

    pub(in crate::driver::rpc) fn matches(
        &self,
        plan: &DeleteConsumerGroupOffsetsPlan,
        result_limit: usize,
    ) -> bool {
        self.plan == *plan && self.result_limit == result_limit
    }

    pub(in crate::driver::rpc) const fn plan(&self) -> &DeleteConsumerGroupOffsetsPlan {
        &self.plan
    }

    pub(in crate::driver::rpc) const fn result_limit(&self) -> usize {
        self.result_limit
    }

    pub(in crate::driver::rpc) fn into_parts(self) -> (DeleteConsumerGroupOffsetsPlan, usize) {
        (self.plan, self.result_limit)
    }
}
