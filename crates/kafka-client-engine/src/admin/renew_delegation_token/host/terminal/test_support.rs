//! Test-only observations of recovered secret call-and-correlation ownership.

use kafka_client_core::RenewDelegationTokenPlan;

use super::super::{RenewDelegationTokenHost, RenewDelegationTokenHostError};

impl RenewDelegationTokenHost {
    pub(in crate::admin::renew_delegation_token) fn retain_recovered_call_for_test(
        &mut self,
        plan: RenewDelegationTokenPlan,
    ) {
        self.operations[0].recovered_call = Some(
            crate::driver::RecoveredRenewDelegationTokenCall::for_test(plan),
        );
    }

    pub(in crate::admin::renew_delegation_token) fn recovered_ownership_matches_for_test(
        &self,
        expected_hmac: &[u8],
        expected_period_ms: Option<i64>,
    ) -> bool {
        self.operations[0]
            .recovered_call
            .as_ref()
            .is_some_and(|recovered| {
                recovered.matches_correlation_for_test(expected_hmac, expected_period_ms)
            })
    }

    pub(in crate::admin::renew_delegation_token) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), RenewDelegationTokenHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::renew_delegation_token) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), RenewDelegationTokenHostError> {
        self.publish_terminal(0)
    }
}
