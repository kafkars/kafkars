//! Embedded driver construction, wake, bounded-turn, and shutdown scenarios.

use std::time::Duration;

use crate::EngineConfig;

use super::{DriverOwnerError, DriverTurn, owner::DriverOwner};

#[test]
fn one_owner_builds_the_driver_handle_reactor_and_wake_source() {
    let mut owner = owner();
    let wake = owner.reactor_wake();

    assert!(!owner.is_shutdown());
    assert!(wake.request().is_ok());
    owner.close_admission();
    let outcome = owner
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("drive bounded shutdown turn: {error}"));
    assert_eq!(outcome, DriverTurn::Shutdown);
    assert!(owner.is_shutdown());
}

#[test]
fn dropping_the_sole_driver_handle_uses_implicit_reactor_shutdown() {
    let mut owner = owner();
    owner.close_admission();

    let outcome = owner
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("drive bounded shutdown turn: {error}"));

    assert_eq!(outcome, DriverTurn::Shutdown);
    owner.close_admission();
    assert!(owner.is_shutdown());
}

#[test]
fn implicit_shutdown_respects_the_engine_turn_bound() {
    let mut owner = owner();

    let error = owner
        .shutdown_with_turn_limit(0, Duration::ZERO)
        .err()
        .unwrap_or_else(|| panic!("zero shutdown turns must preserve the outer bound"));

    assert!(matches!(error, DriverOwnerError::ShutdownTurnExhausted));
}

#[test]
fn endpoint_failures_do_not_acquire_a_partial_driver_owner() {
    let config = EngineConfig::new(vec!["unbracketed::ipv6:9092".to_owned()]);

    let error = DriverOwner::build(&config)
        .err()
        .unwrap_or_else(|| panic!("ambiguous endpoint must be rejected"));

    assert!(matches!(error, DriverOwnerError::Endpoint { index: 0, .. }));
}

#[test]
fn driver_bootstrap_capacity_bounds_construction_before_reactor_acquisition() {
    let config = EngineConfig::new(vec!["broker.test:9092".to_owned(); 17]);

    let error = DriverOwner::build(&config)
        .err()
        .unwrap_or_else(|| panic!("driver bootstrap capacity must be preserved"));

    assert!(matches!(error, DriverOwnerError::Bootstrap(_)));
}

fn owner() -> DriverOwner {
    let config = EngineConfig::new(vec!["127.0.0.1:1".to_owned()]);
    DriverOwner::build(&config)
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"))
}
