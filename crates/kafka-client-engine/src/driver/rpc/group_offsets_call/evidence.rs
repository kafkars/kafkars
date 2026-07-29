//! Exact group-offset query identity retained across every driver ownership state.

use kafka_client_core::ListConsumerGroupOffsetsPlan;

#[derive(Debug)]
pub(in crate::driver::rpc) struct GroupOffsetsEvidence {
    plan: ListConsumerGroupOffsetsPlan,
    result_limit: usize,
}

impl GroupOffsetsEvidence {
    pub(in crate::driver::rpc) const fn new(
        plan: ListConsumerGroupOffsetsPlan,
        result_limit: usize,
    ) -> Self {
        Self { plan, result_limit }
    }

    pub(in crate::driver::rpc) fn matches(
        &self,
        plan: &ListConsumerGroupOffsetsPlan,
        result_limit: usize,
    ) -> bool {
        self.plan == *plan && self.result_limit == result_limit
    }

    pub(in crate::driver::rpc) const fn plan(&self) -> &ListConsumerGroupOffsetsPlan {
        &self.plan
    }

    pub(in crate::driver::rpc) const fn result_limit(&self) -> usize {
        self.result_limit
    }

    pub(in crate::driver::rpc) fn into_parts(self) -> (ListConsumerGroupOffsetsPlan, usize) {
        (self.plan, self.result_limit)
    }
}
