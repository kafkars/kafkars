//! Host scheduling, contention, close, and fault evidence for one assigned shard.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{PartitionIndex, StartPosition};

use crate::{
    EngineConfig,
    clock::MonotonicClock,
    consumer::{
        AssignedConsumerCompletionNotifier, AssignedConsumerFaultKind, AssignedPartitionInput,
    },
    driver::DriverOwner,
};

use super::{
    EngineHostError, assigned_consumer::drive_shard,
    assigned_consumer_start::start_assigned_consumer,
};

#[test]
fn contention_is_blocked_without_false_progress() {
    let (mut driver, clock, shard, _port, mut notifier) = setup();
    let guard = shard.lock_for_test();
    let progress = drive_shard(
        &shard,
        &driver,
        false,
        clock.now().unwrap_or_else(|error| panic!("clock: {error}")),
    )
    .unwrap_or_else(|error| panic!("contended turn: {error}"));

    assert_eq!(progress.unsettled, usize::MAX);
    assert!(!progress.progressed);
    assert!(progress.blocked_work);
    drop(guard);
    shutdown(&mut driver, &mut notifier);
}

#[test]
fn each_host_call_runs_one_bounded_owner_turn() {
    let (mut driver, clock, shard, port, mut notifier) = setup();
    let _accepted = port
        .replace_assignment(
            vec![AssignedPartitionInput::new(
                Arc::from("orders"),
                PartitionIndex::from_raw(0),
                StartPosition::Offset(offset(4)),
            )],
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    let before = shard
        .try_with_owner(|owner| owner.pending_effect_count_for_test())
        .unwrap_or_else(|error| panic!("owner slot: {error:?}"));

    let first = drive_shard(
        &shard,
        &driver,
        false,
        clock.now().unwrap_or_else(|error| panic!("clock: {error}")),
    )
    .unwrap_or_else(|error| panic!("first turn: {error}"));

    assert!(first.progressed);
    assert!(first.unsettled > 0);
    let after = shard
        .try_with_owner(|owner| owner.pending_effect_count_for_test())
        .unwrap_or_else(|error| panic!("owner slot: {error:?}"));
    assert_eq!(after.saturating_add(1), before);
    shutdown(&mut driver, &mut notifier);
}

#[test]
fn shutdown_waits_for_core_authorized_close_retention() {
    let (mut driver, clock, shard, _port, mut notifier) = setup();
    let mut completed = false;
    for _attempt in 0..8 {
        let progress = drive_shard(
            &shard,
            &driver,
            true,
            clock.now().unwrap_or_else(|error| panic!("clock: {error}")),
        )
        .unwrap_or_else(|error| panic!("close turn: {error}"));
        completed = progress.close_completed && progress.unsettled == 0;
        if completed {
            break;
        }
    }

    assert!(completed);
    shutdown(&mut driver, &mut notifier);
}

#[test]
fn retained_owner_fault_is_a_terminal_host_error() {
    let (mut driver, clock, shard, _port, mut notifier) = setup();
    shard
        .try_with_owner(crate::consumer::AssignedConsumerOwner::install_fault_for_test)
        .unwrap_or_else(|error| panic!("owner slot: {error:?}"));

    let error = drive_shard(
        &shard,
        &driver,
        false,
        clock.now().unwrap_or_else(|error| panic!("clock: {error}")),
    )
    .err()
    .unwrap_or_else(|| panic!("faulted owner must fail host"));

    assert!(matches!(
        error,
        EngineHostError::AssignedConsumerFault(AssignedConsumerFaultKind::Clock)
    ));
    shutdown(&mut driver, &mut notifier);
}

#[test]
fn leased_delivery_blocks_close_until_the_unique_port_reclaims_it() {
    let (mut driver, clock, shard, port, mut notifier) = setup();
    let _accepted = port
        .replace_assignment(
            vec![AssignedPartitionInput::new(
                Arc::from("orders"),
                PartitionIndex::from_raw(0),
                StartPosition::Offset(offset(10)),
            )],
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    shard
        .try_with_owner(|owner| {
            assert!(owner.turn(&driver).progressed());
            owner.install_ready_delivery_for_test(10);
        })
        .unwrap_or_else(|error| panic!("install delivery: {error:?}"));
    let delivery = port
        .take_delivery()
        .unwrap_or_else(|error| panic!("take delivery: {error:?}"))
        .unwrap_or_else(|| panic!("ready delivery"));
    for _attempt in 0..4 {
        let progress = drive_shard(
            &shard,
            &driver,
            true,
            clock.now().unwrap_or_else(|error| panic!("clock: {error}")),
        )
        .unwrap_or_else(|error| panic!("close turn: {error}"));
        assert!(!progress.close_completed);
    }

    let reclaimed = port
        .reclaim_delivery(delivery)
        .unwrap_or_else(|_rejection| panic!("reclaim reaches owner"));
    assert_eq!(reclaimed.into_value(), Ok(()));
    let mut completed = false;
    for _attempt in 0..8 {
        let progress = drive_shard(
            &shard,
            &driver,
            true,
            clock.now().unwrap_or_else(|error| panic!("clock: {error}")),
        )
        .unwrap_or_else(|error| panic!("post-reclaim close turn: {error}"));
        completed = progress.close_completed && progress.unsettled == 0;
        if completed {
            break;
        }
    }
    assert!(completed);
    shutdown(&mut driver, &mut notifier);
}

fn setup() -> (
    DriverOwner,
    Arc<MonotonicClock>,
    crate::consumer::AssignedConsumerShardOwner,
    crate::consumer::AssignedConsumerPort,
    AssignedConsumerCompletionNotifier,
) {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build driver: {error}"));
    let clock = Arc::new(MonotonicClock::new());
    let (notifier, publishers) = AssignedConsumerCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("assigned-consumer notifier: {error}"));
    let (owner, port) = start_assigned_consumer(
        Arc::clone(&clock),
        Arc::new(driver.reactor_wake()),
        publishers.close,
        publishers.recv,
    )
    .unwrap_or_else(|error| panic!("assigned consumer: {error:?}"));
    (driver, clock, owner, port, notifier)
}

fn shutdown(driver: &mut DriverOwner, notifier: &mut AssignedConsumerCompletionNotifier) {
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown: {error}"));
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("assigned-consumer notifier stop: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("assigned-consumer notifier join: {error}"));
}

fn offset(value: i64) -> kafka_client_core::NextFetchOffset {
    kafka_client_core::NextFetchOffset::try_from_raw(value)
        .unwrap_or_else(|| panic!("nonnegative offset"))
}
