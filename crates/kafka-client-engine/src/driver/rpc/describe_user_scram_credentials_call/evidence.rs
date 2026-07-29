//! Exact SCRAM user selection and retained bounds across linear call states.

use kafka_client_core::DescribeUserScramCredentialsPlan;

#[derive(Debug)]
pub(in crate::driver::rpc) struct DescribeUserScramCredentialsEvidence {
    plan: DescribeUserScramCredentialsPlan,
    request_limit: usize,
    result_limit: usize,
}

impl DescribeUserScramCredentialsEvidence {
    pub(in crate::driver::rpc) const fn new(
        plan: DescribeUserScramCredentialsPlan,
        request_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            plan,
            request_limit,
            result_limit,
        }
    }

    pub(in crate::driver::rpc) const fn plan(&self) -> &DescribeUserScramCredentialsPlan {
        &self.plan
    }

    pub(in crate::driver::rpc) const fn result_limit(&self) -> usize {
        self.result_limit
    }

    pub(in crate::driver::rpc) fn matches(
        &self,
        plan: &DescribeUserScramCredentialsPlan,
        request_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.plan == *plan
            && self.request_limit == request_limit
            && self.result_limit == result_limit
    }

    pub(in crate::driver::rpc) fn into_parts(
        self,
    ) -> (DescribeUserScramCredentialsPlan, usize, usize) {
        (self.plan, self.request_limit, self.result_limit)
    }
}
