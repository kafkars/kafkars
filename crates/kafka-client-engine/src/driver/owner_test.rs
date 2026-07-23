//! Embedded driver construction, wake, bounded-turn, and shutdown scenarios.

use std::time::Duration;

use kafka_driver::{SubmitError, TurnOutcome};

use crate::{EngineConfig, producer::ingress::ProducerShardWake};

use super::{DriverOwnerError, owner::DriverOwner};

#[test]
fn one_owner_builds_the_driver_handle_reactor_and_wake_source() {
    let mut owner = owner();
    let wake = owner.producer_wake();

    assert!(!owner.is_shutdown());
    assert!(wake.wake().is_ok());
    let barrier = owner
        .begin_shutdown()
        .unwrap_or_else(|error| panic!("admit owner shutdown barrier: {error}"));
    let outcome = owner
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("drive bounded shutdown turn: {error}"));
    assert_eq!(outcome, TurnOutcome::Shutdown { commands: 1 });
    assert_eq!(barrier.wait(), Ok(()));
    assert!(owner.is_shutdown());
}

#[test]
fn priority_shutdown_is_one_bounded_turn_and_one_terminal_barrier() {
    let mut owner = owner();
    let barrier = owner
        .begin_shutdown()
        .unwrap_or_else(|error| panic!("admit driver shutdown barrier: {error}"));

    let outcome = owner
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("drive bounded shutdown turn: {error}"));

    assert_eq!(outcome, TurnOutcome::Shutdown { commands: 1 });
    assert_eq!(barrier.wait(), Ok(()));
    assert!(matches!(owner.begin_shutdown(), Err(SubmitError::Closed)));
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
