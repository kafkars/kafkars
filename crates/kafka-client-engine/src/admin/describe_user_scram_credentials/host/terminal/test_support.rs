//! Test-only observations of retained SCRAM credential-description ownership.

use kafka_client_core::DescribeUserScramCredentialsPlan;

use super::super::{DescribeUserScramCredentialsHost, DescribeUserScramCredentialsHostError};

impl DescribeUserScramCredentialsHost {
    pub(in crate::admin::describe_user_scram_credentials) const fn retained_bytes_for_test(
        &self,
    ) -> usize {
        self.retained_bytes
    }

    pub(in crate::admin::describe_user_scram_credentials) fn retain_recovered_call_for_test(
        &mut self,
        plan: DescribeUserScramCredentialsPlan,
        request_limit: usize,
        result_limit: usize,
    ) {
        self.operations[0].recovered_call = Some(
            crate::driver::RecoveredDescribeUserScramCredentialsCall::for_test(
                plan,
                request_limit,
                result_limit,
            ),
        );
    }

    pub(in crate::admin::describe_user_scram_credentials) fn recovered_call_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    pub(in crate::admin::describe_user_scram_credentials) fn recovered_matches_for_test(
        &self,
        plan: &DescribeUserScramCredentialsPlan,
        request_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.operations[0]
            .recovered_call
            .as_ref()
            .is_some_and(|recovered| recovered.matches_evidence(plan, request_limit, result_limit))
    }

    pub(in crate::admin::describe_user_scram_credentials) fn bounds_for_test(
        &self,
    ) -> (usize, usize) {
        (
            self.operations[0].bounds.request_limit,
            self.operations[0].bounds.result_limit,
        )
    }

    pub(in crate::admin::describe_user_scram_credentials) fn replace_call_with_raw_for_test(
        &mut self,
        plan: DescribeUserScramCredentialsPlan,
        request_limit: usize,
        result_limit: usize,
    ) {
        drop(self.operations[0].call.take());
        self.operations[0].raw_terminal = Some(
            crate::driver::DescribeUserScramCredentialsRawTerminal::for_test(
                plan,
                request_limit,
                result_limit,
            ),
        );
    }

    pub(in crate::admin::describe_user_scram_credentials) fn raw_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].raw_terminal.is_some()
    }

    pub(in crate::admin::describe_user_scram_credentials) fn settle_raw_for_test(
        &mut self,
    ) -> Result<(), DescribeUserScramCredentialsHostError> {
        self.settle_raw(0)
    }

    pub(in crate::admin::describe_user_scram_credentials) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), DescribeUserScramCredentialsHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::describe_user_scram_credentials) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), DescribeUserScramCredentialsHostError> {
        self.publish_terminal(0)
    }
}
