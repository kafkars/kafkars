//! Admin broker-unregistration entry-point surface tests.

use super::Admin;
use crate::admin::UnregisterBrokerBuilder;

#[test]
fn broker_unregistration_starts_as_an_inert_builder() {
    let method: fn(&Admin, i32) -> UnregisterBrokerBuilder = Admin::unregister_broker;

    let _ = method;
}
