//! Share-registry host quiescence and shard-contention scenarios.

use std::{sync::Arc, time::Duration};

use kafka_client_core::Moment;

use crate::{
    EngineConfig,
    consumer::{
        ShareConsumerRegistry, ShareConsumerShardOwner, ShareConsumerShardWake,
        ShareConsumerShardWakeError,
    },
    driver::DriverOwner,
};

use super::drive::{drive_registry, drive_shard};

struct NoopWake;

impl ShareConsumerShardWake for NoopWake {
    fn request_share_turn(&self) -> Result<(), ShareConsumerShardWakeError> {
        Ok(())
    }
}

#[test]
fn empty_share_registry_reports_exact_quiescence() {
    let (mut registry, mut driver) = setup();
    let progress = drive_registry(
        &mut registry,
        &crate::clock::MonotonicClock::new(),
        &driver,
        Moment::from_tick(0),
    )
    .unwrap_or_else(|error| panic!("share turn: {error}"));

    assert_eq!(progress.unsettled, 0);
    assert!(!progress.progressed);
    assert!(!progress.blocked_work);
    assert_eq!(progress.next_deadline, None);
    stop_driver(&mut driver);
}

#[test]
fn contended_share_shard_cannot_look_quiescent_to_shutdown() {
    let (registry, mut driver) = setup();
    let clock = Arc::new(crate::clock::MonotonicClock::new());
    let owner = ShareConsumerShardOwner::new(registry, Arc::clone(&clock), Arc::new(NoopWake));
    let lock = owner.lock_registry_for_test();

    let progress = drive_shard(&owner, &clock, &driver, true, Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("share shard turn: {error}"));

    assert_eq!(progress.unsettled, usize::MAX);
    assert!(!progress.progressed);
    assert!(progress.blocked_work);
    assert_eq!(progress.next_deadline, None);
    drop(lock);
    stop_driver(&mut driver);
}

#[test]
fn shutdown_closes_and_removes_registered_share_member() {
    let (registry, mut driver) = setup();
    let clock = Arc::new(crate::clock::MonotonicClock::new());
    let owner = ShareConsumerShardOwner::new(registry, Arc::clone(&clock), Arc::new(NoopWake));
    let port = owner.admission_port();
    let _registration = port
        .try_register(Arc::from("workers"), None, vec![Arc::from("jobs")])
        .unwrap_or_else(|_error| panic!("register"));
    port.request_control_close(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("control close: {error:?}"));

    let first = drive_shard(&owner, &clock, &driver, true, Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("first close turn: {error}"));
    assert_eq!(first.unsettled, 1);
    assert!(first.progressed);
    let second = drive_shard(&owner, &clock, &driver, true, Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("second close turn: {error}"));
    assert_eq!(second.unsettled, 0);
    assert!(second.progressed);
    stop_driver(&mut driver);
}

fn setup() -> (ShareConsumerRegistry, DriverOwner) {
    let registry =
        ShareConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error}"));
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    (registry, driver)
}

fn stop_driver(driver: &mut DriverOwner) {
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown: {error}"));
}
