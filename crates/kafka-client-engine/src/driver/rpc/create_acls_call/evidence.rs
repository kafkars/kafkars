//! Exact ACL submission identity retained across driver ownership states.

use kafka_client_core::CreateAclsPlan;

#[derive(Debug)]
pub(in crate::driver::rpc) struct CreateAclsEvidence {
    plan: CreateAclsPlan,
    request_limit: usize,
    result_limit: usize,
}

impl CreateAclsEvidence {
    pub(in crate::driver::rpc) const fn new(
        plan: CreateAclsPlan,
        request_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            plan,
            request_limit,
            result_limit,
        }
    }

    pub(in crate::driver::rpc) fn matches(
        &self,
        plan: &CreateAclsPlan,
        request_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.plan == *plan
            && self.request_limit == request_limit
            && self.result_limit == result_limit
    }

    pub(in crate::driver::rpc) const fn plan(&self) -> &CreateAclsPlan {
        &self.plan
    }

    pub(in crate::driver::rpc) const fn result_limit(&self) -> usize {
        self.result_limit
    }

    pub(in crate::driver::rpc) fn into_parts(self) -> (CreateAclsPlan, usize, usize) {
        (self.plan, self.request_limit, self.result_limit)
    }
}
