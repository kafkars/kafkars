//! Broker-unregistration plan validation scenarios.

use super::{UnregisterBrokerPlan, UnregisterBrokerPlanError};

#[test]
fn plan_preserves_one_nonnegative_broker_identity() {
    let zero = UnregisterBrokerPlan::new(0).unwrap_or_else(|error| panic!("zero: {error}"));
    let largest =
        UnregisterBrokerPlan::new(i32::MAX).unwrap_or_else(|error| panic!("largest: {error}"));

    assert_eq!(zero.broker_id(), 0);
    assert_eq!(largest.into_broker_id(), i32::MAX);
}

#[test]
fn negative_broker_identity_is_rejected_before_machine_construction() {
    assert_eq!(
        UnregisterBrokerPlan::new(-1),
        Err(UnregisterBrokerPlanError::NegativeBrokerId)
    );
}
