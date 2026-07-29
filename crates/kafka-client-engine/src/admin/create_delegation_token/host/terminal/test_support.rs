//! Test-only observations of retained token-creation call ownership.

use super::super::{CreateDelegationTokenHost, CreateDelegationTokenHostError};

impl CreateDelegationTokenHost {
    pub(in crate::admin::create_delegation_token) fn retain_recovered_call_for_test(&mut self) {
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredCreateDelegationTokenCall::for_test());
    }

    pub(in crate::admin::create_delegation_token) fn recovered_call_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    pub(in crate::admin::create_delegation_token) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), CreateDelegationTokenHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::create_delegation_token) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), CreateDelegationTokenHostError> {
        self.publish_terminal(0)
    }
}
