//! Non-secret attempt identity and capacity evidence for one SCRAM mutation.

use kafka_client_core::AlterUserScramCredentialsPlan;

#[derive(Debug)]
pub(in crate::driver::rpc) struct AlterUserScramCredentialsEvidence {
    plan: AlterUserScramCredentialsPlan,
    prepared_request_bytes: usize,
    result_limit: usize,
}

impl AlterUserScramCredentialsEvidence {
    pub(in crate::driver::rpc) const fn new(
        plan: AlterUserScramCredentialsPlan,
        prepared_request_bytes: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            plan,
            prepared_request_bytes,
            result_limit,
        }
    }

    pub(in crate::driver::rpc) const fn plan(&self) -> &AlterUserScramCredentialsPlan {
        &self.plan
    }

    pub(in crate::driver::rpc) fn matches(
        &self,
        plan: &AlterUserScramCredentialsPlan,
        prepared_request_bytes: usize,
        result_limit: usize,
    ) -> bool {
        self.plan == *plan
            && self.prepared_request_bytes == prepared_request_bytes
            && self.result_limit == result_limit
    }

    pub(in crate::driver::rpc) fn into_parts(
        self,
    ) -> (AlterUserScramCredentialsPlan, usize, usize) {
        (self.plan, self.prepared_request_bytes, self.result_limit)
    }
}
