//! Private group-registry host scheduling and shutdown-fencing scenarios.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{ClassicGroupTiming, ClassicHeartbeatPolicy, ClassicRejoinPolicy, Moment};

use crate::{
    EngineConfig,
    consumer::{
        GroupConsumerRegistry, GroupConsumerShardOwner, GroupConsumerShardWake,
        GroupConsumerShardWakeError,
    },
    driver::DriverOwner,
};

use super::group_consumer::{drive_registry, drive_shard};

struct NoopWake;

impl GroupConsumerShardWake for NoopWake {
    fn request_group_turn(&self) -> Result<(), GroupConsumerShardWakeError> {
        Ok(())
    }
}

#[test]
fn one_idle_registry_turn_reports_exact_quiescence() {
    let (mut registry, mut driver) = setup();
    let clock = crate::clock::MonotonicClock::new();

    let progress = drive_registry(&mut registry, &clock, &driver, false, Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("group turn: {error}"));

    assert_eq!(progress.unsettled, 0);
    assert!(!progress.progressed);
    assert!(!progress.blocked_work);
    assert_eq!(progress.next_deadline, None);
    shutdown(&mut registry, &mut driver);
}

#[test]
fn shutdown_fences_registry_admission_before_the_bounded_turn() {
    let (mut registry, mut driver) = setup();
    let clock = crate::clock::MonotonicClock::new();

    let progress = drive_registry(&mut registry, &clock, &driver, true, Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("group shutdown turn: {error}"));

    assert_eq!(progress.unsettled, 0);
    assert!(!progress.blocked_work);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown: {error}"));
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("shutdown drive must close admission: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("group join: {error}"));
}

#[test]
fn shard_admission_deadline_is_scheduled_on_the_embedded_host_turn() {
    let registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("group registry: {error}"));
    let clock = Arc::new(crate::clock::MonotonicClock::new());
    let (owner, port) =
        GroupConsumerShardOwner::new(registry, Arc::clone(&clock), Arc::new(NoopWake));
    let group_id = port
        .try_register(
            Arc::from("workers"),
            vec![Arc::from("orders")],
            timing(),
            heartbeat_policy(),
            rejoin_policy(),
        )
        .unwrap_or_else(|failure| panic!("registration failed: {:?}", failure.kind));
    let _admission = port
        .begin_cycle(group_id, Duration::from_nanos(1))
        .unwrap_or_else(|error| panic!("cycle admission failed: {error:?}"));
    let due = {
        let registry = owner
            .try_registry()
            .unwrap_or_else(|error| panic!("group registry lock failed: {error:?}"));
        registry.next_deadline().map_or_else(
            || panic!("accepted cycle deadline expected"),
            |deadline| Moment::from_tick(deadline.tick()),
        )
    };
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));

    let progress = drive_shard(&owner, &clock, &driver, false, due)
        .unwrap_or_else(|error| panic!("group shard turn: {error}"));

    assert!(progress.progressed);
    assert_eq!(progress.unsettled, 0);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown: {error}"));
    let mut registry = owner.terminal_registry();
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("group recovery: {error}"));
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("group stop: {error}"));
    drop(registry);
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("group join: {error}"));
}

fn timing() -> ClassicGroupTiming {
    ClassicGroupTiming::try_new(12_345, 54_321)
        .unwrap_or_else(|error| panic!("valid classic group timing: {error}"))
}

fn heartbeat_policy() -> ClassicHeartbeatPolicy {
    ClassicHeartbeatPolicy::try_new(1_000_000_000, 2_000_000_000)
        .unwrap_or_else(|error| panic!("valid heartbeat policy: {error}"))
}

fn rejoin_policy() -> ClassicRejoinPolicy {
    ClassicRejoinPolicy::try_new(1_000_000_000, 30_000_000_000)
        .unwrap_or_else(|error| panic!("valid rejoin policy: {error:?}"))
}

#[test]
fn contended_group_shard_cannot_look_quiescent_to_shutdown() {
    let registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("group registry: {error}"));
    let clock = Arc::new(crate::clock::MonotonicClock::new());
    let (owner, _port) =
        GroupConsumerShardOwner::new(registry, Arc::clone(&clock), Arc::new(NoopWake));
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    let lock = owner.lock_registry_for_test();

    let progress = drive_shard(&owner, &clock, &driver, true, Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("contended turn failed: {error}"));

    assert_eq!(progress.unsettled, usize::MAX);
    assert!(!progress.progressed);
    assert!(progress.blocked_work);
    assert_eq!(progress.next_deadline, None);
    drop(lock);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown: {error}"));
    let mut registry = owner.terminal_registry();
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("group recovery: {error}"));
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("group stop: {error}"));
    drop(registry);
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("group join: {error}"));
}

#[test]
fn contended_port_defers_one_host_registry_reacquisition() {
    let registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("group registry: {error}"));
    let clock = Arc::new(crate::clock::MonotonicClock::new());
    let (owner, port) =
        GroupConsumerShardOwner::new(registry, Arc::clone(&clock), Arc::new(NoopWake));
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    let lock = owner.lock_registry_for_test();

    let _failure = port
        .try_register(
            Arc::from("workers"),
            vec![Arc::from("orders")],
            timing(),
            heartbeat_policy(),
            rejoin_policy(),
        )
        .err()
        .unwrap_or_else(|| panic!("contended port registration must reject"));
    drop(lock);

    let handoff = drive_shard(&owner, &clock, &driver, false, Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("group handoff turn failed: {error}"));
    assert_eq!(handoff.unsettled, usize::MAX);
    assert!(!handoff.progressed);
    assert!(handoff.blocked_work);
    assert_eq!(handoff.next_deadline, None);

    let resumed = drive_shard(&owner, &clock, &driver, false, Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("group resumed turn failed: {error}"));
    assert_eq!(resumed.unsettled, 0);
    assert!(!resumed.progressed);
    assert!(!resumed.blocked_work);
    assert_eq!(resumed.next_deadline, None);

    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown: {error}"));
    let mut registry = owner.terminal_registry();
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("group recovery: {error}"));
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("group stop: {error}"));
    drop(registry);
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("group join: {error}"));
}

fn setup() -> (GroupConsumerRegistry, DriverOwner) {
    let registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("group registry: {error}"));
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    (registry, driver)
}

fn shutdown(registry: &mut GroupConsumerRegistry, driver: &mut DriverOwner) {
    registry.close_admission();
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown: {error}"));
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("group recovery: {error}"));
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("group stop: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("group join: {error}"));
}
