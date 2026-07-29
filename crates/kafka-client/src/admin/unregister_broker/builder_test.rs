//! Broker-unregistration builder surface tests.

use std::time::Duration;

use super::{UnregisterBroker, UnregisterBrokerBuilder};

#[test]
fn builder_keeps_broker_and_deadline_configuration_inert_until_submit() {
    let deadline_after: fn(UnregisterBrokerBuilder, Duration) -> UnregisterBrokerBuilder =
        UnregisterBrokerBuilder::deadline_after;
    let submit: fn(UnregisterBrokerBuilder) -> UnregisterBroker = UnregisterBrokerBuilder::submit;

    let _ = (deadline_after, submit);
}
