//! Test-only observations of recovered call and owner-selection ownership.

use kafka_client_core::DescribeDelegationTokensPlan;

use super::super::{DescribeDelegationTokensHost, DescribeDelegationTokensHostError};

impl DescribeDelegationTokensHost {
    pub(in crate::admin::describe_delegation_tokens) fn accepted_call_and_correlation_are_retained_for_test(
        &self,
    ) -> bool {
        let Some(plan) = self.operations[0].correlation_plan.as_ref() else {
            return false;
        };
        let kafka_client_core::DescribeDelegationTokensSelection::Owners(owners) = plan.selection()
        else {
            return false;
        };
        self.operations[0].call.is_some()
            && owners.len() == 1
            && owners[0].principal_type() == "User"
            && owners[0].principal_name() == "alice"
    }

    pub(in crate::admin::describe_delegation_tokens) fn retain_recovered_call_for_test(
        &mut self,
        plan: DescribeDelegationTokensPlan,
    ) {
        self.operations[0].correlation_plan = None;
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredDescribeDelegationTokensCall::for_test(plan));
    }

    pub(in crate::admin::describe_delegation_tokens) fn recovered_ownership_is_retained_for_test(
        &self,
    ) -> bool {
        let Some(recovered) = self.operations[0].recovered_call.as_ref() else {
            return false;
        };
        let kafka_client_core::DescribeDelegationTokensSelection::Owners(owners) =
            recovered.plan().selection()
        else {
            return false;
        };
        owners.len() == 1
            && owners[0].principal_type() == "User"
            && owners[0].principal_name() == "alice"
    }

    pub(in crate::admin::describe_delegation_tokens) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), DescribeDelegationTokensHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::describe_delegation_tokens) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), DescribeDelegationTokensHostError> {
        self.publish_terminal(0)
    }
}
