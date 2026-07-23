//! Host wait selection and shard-wide terminal-refusal scenarios.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use kafka_client_core::{Deadline, Moment, PartitionIndex};

use crate::{
    EngineConfig,
    clock::{MonotonicClock, OperationDeadline},
    driver::DriverOwner,
    producer::{
        ProducerHost, ProducerRecord,
        host_turn::ProducerTurnOutcome,
        ingress::{ProducerShardOwner, ProducerShardStats},
        pending::PendingSendRegistration,
    },
};

use super::{
    EngineHostControl, EngineHostError, EngineHostResources, recover,
    runner::{prepare_notification_stop, producer_wait},
};

#[test]
fn prepared_submission_parks_on_its_original_deadline() {
    let outcome = outcome(Some(Deadline::from_tick(250)), false, false);

    assert_eq!(
        producer_wait(Moment::from_tick(100), Some(outcome), false),
        Duration::from_nanos(150)
    );
}

#[test]
fn every_nonzero_wait_is_capped_for_failed_wake_liveness() {
    let outcome = outcome(Some(Deadline::from_tick(1_000_000_000)), false, false);

    assert_eq!(
        producer_wait(Moment::from_tick(0), Some(outcome), false),
        Duration::from_millis(100)
    );
}

#[test]
fn only_runnable_or_driver_local_work_requests_an_immediate_turn() {
    let runnable = outcome(None, true, false);
    assert_eq!(
        producer_wait(Moment::from_tick(0), Some(runnable), false),
        Duration::ZERO
    );
    let idle = outcome(None, false, false);
    assert_eq!(
        producer_wait(Moment::from_tick(0), Some(idle), true),
        Duration::ZERO
    );
}

#[test]
fn transient_lock_or_notification_work_uses_the_liveness_cap() {
    let blocked = outcome(None, false, true);

    assert_eq!(
        producer_wait(Moment::from_tick(0), Some(blocked), false),
        Duration::from_millis(100)
    );
}

#[test]
fn normal_stop_refuses_live_pending_ownership_while_resources_remain_retained() {
    let resources = resources();
    let waiting = register_pending(&resources);
    let before = shard_stats(&resources);

    let Err(error) = prepare_notification_stop(&resources) else {
        panic!("normal notification preparation must refuse pending ownership")
    };
    assert!(
        error
            .to_string()
            .contains("pending producer ownership remains")
    );
    assert_pending_unchanged(before, shard_stats(&resources));
    drop(waiting);
}

#[test]
fn recovery_surfaces_pending_refusal_without_draining_the_retained_resources() {
    let mut resources = resources();
    let waiting = register_pending(&resources);
    let before = shard_stats(&resources);

    let exit = recover(&mut resources, EngineHostError::ForcedTestFailure);

    assert!(exit.notifications.is_none());
    let failure = exit
        .failure
        .unwrap_or_else(|| panic!("pending refusal must remain in the recovery report"));
    assert!(
        failure
            .to_string()
            .contains("pending producer ownership remains")
    );
    assert_pending_unchanged(before, shard_stats(&resources));
    drop(waiting);
}

const fn outcome(
    next_deadline: Option<Deadline>,
    runnable_work: bool,
    blocked_work: bool,
) -> ProducerTurnOutcome {
    ProducerTurnOutcome {
        batch_timers: 0,
        prepared_effects: 0,
        submission_expiries: 0,
        completion_retries: 0,
        reclaim_attempts: 0,
        next_deadline,
        runnable_work,
        blocked_work,
    }
}

fn resources() -> EngineHostResources {
    let config = EngineConfig::new(vec!["192.0.2.1:9092".to_owned()]);
    let validated = config
        .validate()
        .unwrap_or_else(|error| panic!("test engine config should validate: {error:?}"));
    let driver = DriverOwner::build(&config)
        .unwrap_or_else(|error| panic!("test driver should build locally: {error}"));
    let wake = driver.producer_wake();
    let control = Arc::new(EngineHostControl::new(wake.clone()));
    let producer = ProducerHost::new(validated.host_limits)
        .unwrap_or_else(|error| panic!("test producer should start: {error}"));
    EngineHostResources {
        driver,
        producer: ProducerShardOwner::new(producer, Arc::new(wake)),
        clock: Arc::new(MonotonicClock::new()),
        control,
        budget: validated.turn_budget,
    }
}

fn register_pending(resources: &EngineHostResources) -> PendingSendRegistration {
    let mut data = resources
        .producer
        .try_data()
        .unwrap_or_else(|error| panic!("test should acquire shard data: {error:?}"));
    data.register_pending(record(), deadline())
        .unwrap_or_else(|error| panic!("test pending record should register: {error:?}"))
}

fn shard_stats(resources: &EngineHostResources) -> ProducerShardStats {
    resources
        .producer
        .try_data()
        .unwrap_or_else(|error| panic!("test should acquire shard data: {error:?}"))
        .shard_stats()
}

fn assert_pending_unchanged(before: ProducerShardStats, after: ProducerShardStats) {
    assert_eq!(after.pending.records, before.pending.records);
    assert_eq!(after.pending.retained_bytes, before.pending.retained_bytes);
    assert_eq!(
        after.pending.notification_permits,
        before.pending.notification_permits
    );
    assert_eq!(
        after.aggregate_retained_bytes,
        before.aggregate_retained_bytes
    );
    assert_eq!(
        after.host.pending_notification_permits,
        before.host.pending_notification_permits
    );
}

fn record() -> ProducerRecord {
    ProducerRecord::new(
        Arc::from("pending"),
        PartitionIndex::from_raw(0),
        10,
        None,
        Some(Bytes::from_static(b"value")),
    )
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(100), Instant::now())
}
