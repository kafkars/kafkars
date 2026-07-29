//! Test-only observations of recovered call-and-broker ownership.

use kafka_client_core::UnregisterBrokerPlan;

use super::super::{UnregisterBrokerHost, UnregisterBrokerHostError};

impl UnregisterBrokerHost {
    pub(in crate::admin::unregister_broker) fn retain_recovered_call_for_test(
        &mut self,
        plan: UnregisterBrokerPlan,
    ) {
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredUnregisterBrokerCall::for_test(plan));
    }

    pub(in crate::admin::unregister_broker) fn recovered_broker_id_for_test(&self) -> Option<i32> {
        self.operations[0]
            .recovered_call
            .as_ref()
            .map(|recovered| recovered.broker_id_for_test())
    }

    pub(in crate::admin::unregister_broker) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), UnregisterBrokerHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::unregister_broker) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), UnregisterBrokerHostError> {
        self.publish_terminal(0)
    }
}
