//! Exact group-deletion identity retained across every driver ownership state.

use kafka_client_core::{DeleteConsumerGroupsPlan, DeleteConsumerGroupsTarget};

#[derive(Debug)]
pub(in crate::driver::rpc) struct DeleteConsumerGroupsEvidence {
    plan: DeleteConsumerGroupsPlan,
    target: DeleteConsumerGroupsTarget,
    request_limit: usize,
    result_limit: usize,
}

impl DeleteConsumerGroupsEvidence {
    pub(in crate::driver::rpc) const fn new(
        plan: DeleteConsumerGroupsPlan,
        target: DeleteConsumerGroupsTarget,
        request_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            plan,
            target,
            request_limit,
            result_limit,
        }
    }

    pub(in crate::driver::rpc) fn matches(
        &self,
        plan: &DeleteConsumerGroupsPlan,
        target: &DeleteConsumerGroupsTarget,
        request_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.plan == *plan
            && self.target == *target
            && self.request_limit == request_limit
            && self.result_limit == result_limit
    }

    pub(in crate::driver::rpc) const fn target(&self) -> &DeleteConsumerGroupsTarget {
        &self.target
    }

    pub(in crate::driver::rpc) const fn result_limit(&self) -> usize {
        self.result_limit
    }

    pub(in crate::driver::rpc) fn into_parts(
        self,
    ) -> (
        DeleteConsumerGroupsPlan,
        DeleteConsumerGroupsTarget,
        usize,
        usize,
    ) {
        (
            self.plan,
            self.target,
            self.request_limit,
            self.result_limit,
        )
    }
}
