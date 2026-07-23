//! Embedded driver construction, wake, bounded-turn, and shutdown scenarios.

use std::{
    thread,
    time::{Duration, Instant},
};

use crate::EngineConfig;

use super::{DriverOwnerError, DriverTurn, owner::DriverOwner};

const SHUTDOWN_TURN_LIMIT: usize = 64;
const SHUTDOWN_WAIT_LIMIT: Duration = Duration::from_millis(100);
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(1);

#[test]
fn one_owner_builds_the_driver_handle_reactor_and_wake_source() {
    let mut owner = owner();
    let wake = owner.reactor_wake();

    assert!(!owner.is_shutdown());
    assert!(wake.request().is_ok());
    owner
        .close_admission()
        .unwrap_or_else(|error| panic!("request explicit shutdown: {error}"));
    let turns = owner
        .shutdown_with_turn_limit(SHUTDOWN_TURN_LIMIT, SHUTDOWN_WAIT_LIMIT)
        .unwrap_or_else(|error| panic!("drive bounded shutdown turns: {error}"));

    assert!((1..=SHUTDOWN_TURN_LIMIT).contains(&turns));
    assert!(owner.is_shutdown());
}

#[test]
fn retained_explicit_shutdown_barrier_settles_with_the_reactor() {
    let mut owner = owner();
    owner
        .close_admission()
        .unwrap_or_else(|error| panic!("request explicit shutdown: {error}"));

    let first = owner
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("drive bounded shutdown turn: {error}"));

    assert!(matches!(
        first,
        DriverTurn::Progress { .. } | DriverTurn::Shutdown
    ));
    owner
        .close_admission()
        .unwrap_or_else(|error| panic!("repeat explicit shutdown: {error}"));
    let deadline = Instant::now() + SHUTDOWN_DEADLINE;
    while !owner.is_shutdown() {
        owner
            .turn(Duration::ZERO)
            .unwrap_or_else(|error| panic!("drive retained shutdown barrier: {error}"));
        assert!(
            Instant::now() < deadline,
            "explicit driver shutdown exceeded its wall-clock bound"
        );
        thread::yield_now();
    }
    assert!(owner.is_shutdown());
}

#[test]
fn explicit_shutdown_respects_the_engine_turn_bound() {
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
