//! Exact non-member `OffsetCommit` intent and capacity evidence for one attempt.

use kafka_client_core::AlterConsumerGroupOffsetsPlan;

#[derive(Debug)]
pub(in crate::driver::rpc) struct GroupOffsetAlterEvidence {
    plan: AlterConsumerGroupOffsetsPlan,
    request_scratch_limit: usize,
    result_limit: usize,
}

impl GroupOffsetAlterEvidence {
    pub(in crate::driver::rpc) const fn new(
        plan: AlterConsumerGroupOffsetsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            plan,
            request_scratch_limit,
            result_limit,
        }
    }

    pub(in crate::driver::rpc) const fn plan(&self) -> &AlterConsumerGroupOffsetsPlan {
        &self.plan
    }

    pub(in crate::driver::rpc) const fn request_scratch_limit(&self) -> usize {
        self.request_scratch_limit
    }

    pub(in crate::driver::rpc) const fn result_limit(&self) -> usize {
        self.result_limit
    }

    pub(in crate::driver::rpc) fn matches(
        &self,
        plan: &AlterConsumerGroupOffsetsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.plan == *plan
            && self.request_scratch_limit == request_scratch_limit
            && self.result_limit == result_limit
    }

    pub(in crate::driver::rpc) fn into_parts(
        self,
    ) -> (AlterConsumerGroupOffsetsPlan, usize, usize) {
        (self.plan, self.request_scratch_limit, self.result_limit)
    }
}
