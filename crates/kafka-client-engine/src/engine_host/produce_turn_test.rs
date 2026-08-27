//! Scenarios for lock-safe bounded producer and driver turns.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use kafka_client_core::{BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline, Moment};

use crate::{
    EngineConfig, ProducerDeliveryError, ProducerDeliveryFailureKind, ProducerDeliveryStatus,
    clock::OperationDeadline,
    driver::{DriverOwner, TrackedProduceCalls},
    producer::{
        host_limits_test::{start, valid_limits},
        ingress::{CountingWake, ProducerShardOwner},
        materialization::{MaterializationBatch, MaterializationRecord},
    },
    protocol::produce::materialize_explicit_produce_batch,
};

use super::{
    produce::{admit_one, apply_ready},
    produce_turn::{admit_after_partitioning, apply_completions},
};

#[test]
fn matching_retained_partition_lookup_blocks_produce_until_it_settles() {
    let (producer, observer) = super::produce_test::prepared_producer();
    let mut driver = driver();
    let mut calls = TrackedProduceCalls::new(1);
    let mut retry_identity = None;
    let mut data = producer
        .try_data()
        .unwrap_or_else(|error| panic!("lock prepared producer: {error:?}"));
    let exact_deadline = data
        .next_produce_submission_deadline()
        .unwrap_or_else(|| panic!("prepared submission deadline"));

    assert!(
        !admit_after_partitioning(
            &driver,
            &mut calls,
            &mut retry_identity,
            &mut data,
            Moment::from_tick(2),
            Some(exact_deadline),
        )
        .unwrap_or_else(|error| panic!("defer for matching retained lookup: {error}"))
    );
    assert_eq!(calls.retained_count(), 0);
    assert_eq!(data.shard_stats().host.prepared_batches, 1);

    assert!(
        !admit_after_partitioning(
            &driver,
            &mut calls,
            &mut retry_identity,
            &mut data,
            Moment::from_tick(3),
            Some(exact_deadline),
        )
        .unwrap_or_else(|error| panic!("keep deferring retained lookup: {error}"))
    );
    assert_eq!(calls.retained_count(), 0);

    assert!(
        admit_after_partitioning(
            &driver,
            &mut calls,
            &mut retry_identity,
            &mut data,
            Moment::from_tick(4),
            None,
        )
        .unwrap_or_else(|error| panic!("admit after lookup settlement: {error}"))
    );
    assert_eq!(calls.retained_count(), 1);
    drop((data, observer, calls));
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn different_deadline_partition_lookup_does_not_block_ready_produce() {
    let (producer, observer) = super::produce_test::prepared_producer();
    let mut driver = driver();
    let mut calls = TrackedProduceCalls::new(1);
    let mut retry_identity = None;
    let mut data = producer
        .try_data()
        .unwrap_or_else(|error| panic!("lock prepared producer: {error:?}"));
    let ready_deadline = data
        .next_produce_submission_deadline()
        .unwrap_or_else(|| panic!("prepared submission deadline"));
    let unrelated_deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(ready_deadline.core().tick().saturating_add(1)),
        ready_deadline.transport(),
    );
    assert!(
        admit_after_partitioning(
            &driver,
            &mut calls,
            &mut retry_identity,
            &mut data,
            Moment::from_tick(2),
            Some(unrelated_deadline),
        )
        .unwrap_or_else(|error| panic!("unrelated lookup must not defer Produce: {error}"))
    );
    assert_eq!(calls.retained_count(), 1);
    drop((data, observer, calls));
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn completion_polling_waits_without_consuming_when_the_shard_is_contended() {
    let producer =
        ProducerShardOwner::new(start(valid_limits()), Arc::new(CountingWake::default()));
    let mut driver = driver();
    let mut calls = TrackedProduceCalls::new(1);
    calls
        .try_reserve()
        .unwrap_or_else(|| panic!("tracked-call capacity"))
        .submit(
            &driver,
            BatchExecutionId::new(BatchId::from_raw(1), BatchExecutionGeneration::initial()),
            OperationDeadline::from_parts_for_test(
                Deadline::from_tick(50_000_000),
                Instant::now() + Duration::from_millis(50),
            ),
            materialized(),
            Moment::from_tick(0),
        )
        .unwrap_or_else(|error| panic!("tracked Produce admission: {error}"))
        .confirm_receipt();
    drive_until_produce_completion(&mut driver, &mut calls, Moment::from_tick(1));
    let guard = producer
        .try_data()
        .unwrap_or_else(|error| panic!("acquire producer shard: {error:?}"));

    let mut identity_calls = crate::driver::TrackedProducerIdentityCalls::new();
    let mut partitioning_call = None;
    let mut retry_identity_call = None;
    let progress = apply_completions(
        &driver,
        &producer,
        &mut identity_calls,
        &mut partitioning_call,
        &mut retry_identity_call,
        &mut calls,
        Moment::from_tick(1),
    )
    .unwrap_or_else(|error| panic!("contended completion turn: {error}"));

    assert!(!progress);
    drop(guard);
    assert!(
        calls
            .poll_next_ready(Moment::from_tick(1))
            .unwrap_or_else(|error| panic!("ready completion retained: {error}"))
            .is_some()
    );
    calls.discard_settled(Moment::from_tick(12));
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn terminal_fact_is_applied_before_the_tracked_slot_is_released() {
    let (producer, observer) = super::produce_test::prepared_producer();
    let mut driver = driver();
    let mut calls = TrackedProduceCalls::new(1);
    let mut retry_identity = None;
    {
        let mut data = producer
            .try_data()
            .unwrap_or_else(|error| panic!("lock producer shard: {error:?}"));
        admit_one(
            &driver,
            &mut calls,
            &mut retry_identity,
            &mut data,
            Moment::from_tick(2),
        )
        .unwrap_or_else(|error| panic!("tracked Produce admission: {error}"));
    }

    drive_until_produce_completion(&mut driver, &mut calls, Moment::from_tick(500_000_000));
    let mut data = producer
        .try_data()
        .unwrap_or_else(|error| panic!("lock producer shard: {error:?}"));
    assert!(
        apply_ready(
            &driver,
            &mut calls,
            &mut data,
            Moment::from_tick(500_000_000),
            1,
        )
        .unwrap_or_else(|error| panic!("apply Produce terminal: {error}")),
        "ready terminal must settle the tracked call",
    );
    drop(data);
    let Err(ProducerDeliveryError::Failed(failure)) = observer.wait() else {
        panic!("unreachable broker must publish one driver-owned failure")
    };
    assert_eq!(failure.kind(), ProducerDeliveryFailureKind::DeadlineElapsed);
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
    assert!(calls.try_reserve().is_some());
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

fn materialized() -> crate::protocol::produce::MaterializedProduce {
    let batch = MaterializationBatch::try_for_test(
        "orders",
        0,
        vec![MaterializationRecord::new(
            1,
            None,
            Some(Bytes::from_static(b"value")),
            Vec::new(),
        )],
        1_024,
    )
    .unwrap_or_else(|| panic!("test materialization batch must be representable"));
    materialize_explicit_produce_batch(batch)
        .unwrap_or_else(|error| panic!("materialize Produce request: {error}"))
}

fn drive_until_produce_completion(
    driver: &mut DriverOwner,
    calls: &mut TrackedProduceCalls,
    now: Moment,
) {
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut turns = 0_usize;
    loop {
        driver
            .turn(Duration::from_millis(10))
            .unwrap_or_else(|error| panic!("driver turn {turns}: {error}"));
        turns = turns.saturating_add(1);
        if calls
            .poll_next_ready(now)
            .unwrap_or_else(|error| panic!("poll Produce completion: {error}"))
            .is_some()
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "driver must settle the tracked call before the wall-clock bound",
        );
    }
}

fn driver() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver: {error}"))
}
