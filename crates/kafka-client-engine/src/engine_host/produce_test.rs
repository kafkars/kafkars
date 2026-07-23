//! Critical-section evidence for the producer and tracked-driver join.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use kafka_client_core::{ByteCount, Deadline, Moment, ProducerBatchPolicy};

use crate::{
    EngineConfig, ProducerCancellationOutcome, ProducerDeliveryError, ProducerDeliveryFailureKind,
    ProducerDeliveryObserver, ProducerDeliveryStatus,
    clock::OperationDeadline,
    driver::{DriverOwner, TrackedProduceCalls},
    producer::{
        ProducerRecord,
        host_limits_test::{start, valid_limits},
        host_turn::ProducerTurnBudget,
        ingress::{CountingWake, ProducerShardOwner},
    },
};

use super::produce::{admit_one, apply_ready};

const DRIVER_TURN_LIMIT: usize = 256;
const DRIVER_TURN_WAIT: Duration = Duration::from_millis(10);

#[test]
fn accepted_call_is_retained_before_core_reports_submitted() {
    let (producer, observer) = prepared_producer();
    let mut driver = driver();
    let mut calls = TrackedProduceCalls::new(1);
    {
        let mut data = producer
            .try_data()
            .unwrap_or_else(|error| panic!("lock producer shard: {error:?}"));
        assert!(
            admit_one(&driver, &mut calls, &mut data, Moment::from_tick(2))
                .unwrap_or_else(|error| panic!("tracked Produce admission: {error}"))
        );
        assert!(calls.try_reserve().is_none());
    }

    let cancellation = observer
        .try_cancel()
        .unwrap_or_else(|error| panic!("submitted cancellation decision: {error}"));
    assert_eq!(cancellation.outcome(), ProducerCancellationOutcome::TooLate);

    drop((observer, calls));
    shutdown(&mut driver);
}

#[test]
fn immediate_driver_rejection_applies_not_sent_before_unlock() {
    let (producer, observer) = prepared_producer();
    let mut driver = driver();
    shutdown(&mut driver);
    let mut calls = TrackedProduceCalls::new(1);
    let mut data = producer
        .try_data()
        .unwrap_or_else(|error| panic!("lock producer shard: {error:?}"));

    assert!(
        admit_one(&driver, &mut calls, &mut data, Moment::from_tick(2))
            .unwrap_or_else(|error| panic!("driver rejection application: {error}"))
    );
    assert!(calls.try_reserve().is_some());
    drop(data);

    let Err(ProducerDeliveryError::Failed(failure)) = observer.wait() else {
        panic!("driver rejection must publish one failure")
    };
    assert_eq!(failure.kind(), ProducerDeliveryFailureKind::DriverRejected);
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
}

#[test]
fn terminal_fact_is_applied_before_the_tracked_slot_is_released() {
    let (producer, observer) = prepared_producer();
    let mut driver = driver();
    let mut calls = TrackedProduceCalls::new(1);
    {
        let mut data = producer
            .try_data()
            .unwrap_or_else(|error| panic!("lock producer shard: {error:?}"));
        admit_one(&driver, &mut calls, &mut data, Moment::from_tick(2))
            .unwrap_or_else(|error| panic!("tracked Produce admission: {error}"));
    }

    let mut settled = false;
    for turn in 0..DRIVER_TURN_LIMIT {
        driver
            .turn(DRIVER_TURN_WAIT)
            .unwrap_or_else(|error| panic!("driver turn {turn}: {error}"));
        let mut data = producer
            .try_data()
            .unwrap_or_else(|error| panic!("lock producer shard: {error:?}"));
        if apply_ready(&mut calls, &mut data, Moment::from_tick(3), 1)
            .unwrap_or_else(|error| panic!("apply Produce terminal: {error}"))
        {
            settled = true;
            break;
        }
    }

    assert!(settled, "bounded driver turns must settle the tracked call");
    let Err(ProducerDeliveryError::Failed(failure)) = observer.wait() else {
        panic!("unreachable broker must publish one driver-owned failure")
    };
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
    assert!(calls.try_reserve().is_some());
    shutdown(&mut driver);
}

#[test]
fn full_call_capacity_preserves_the_next_prepared_owner() {
    let (producer, first) = prepared_producer();
    let mut driver = driver();
    let mut calls = TrackedProduceCalls::new(1);
    {
        let mut data = producer
            .try_data()
            .unwrap_or_else(|error| panic!("lock producer shard: {error:?}"));
        admit_one(&driver, &mut calls, &mut data, Moment::from_tick(2))
            .unwrap_or_else(|error| panic!("first tracked admission: {error}"));
    }
    let second = admit_prepared(&producer);
    let mut data = producer
        .try_data()
        .unwrap_or_else(|error| panic!("lock producer shard: {error:?}"));
    let before = data.shard_stats().host;

    assert!(
        !admit_one(&driver, &mut calls, &mut data, Moment::from_tick(4))
            .unwrap_or_else(|error| panic!("bounded admission preflight: {error}"))
    );

    let after = data.shard_stats().host;
    assert_eq!(after.prepared_batches, before.prepared_batches);
    assert_eq!(after.prepared_bytes, before.prepared_bytes);
    assert_eq!(after.submission_deadlines, before.submission_deadlines);
    assert_eq!(after.prepared_batches, 1);
    assert_eq!(after.submission_deadlines, 1);
    drop((data, first, second, calls));
    shutdown(&mut driver);
}

fn prepared_producer() -> (ProducerShardOwner, ProducerDeliveryObserver) {
    let producer =
        ProducerShardOwner::new(start(ready_limits()), Arc::new(CountingWake::default()));
    let observer = admit_prepared(&producer);
    (producer, observer)
}

fn admit_prepared(producer: &ProducerShardOwner) -> ProducerDeliveryObserver {
    let accepted = producer
        .admission_port()
        .try_admit_explicit(
            Moment::from_tick(0),
            OperationDeadline::from_parts_for_test(
                Deadline::from_tick(500_000_000),
                Instant::now() + Duration::from_millis(500),
            ),
            ProducerRecord::new(
                Arc::from("orders"),
                kafka_client_core::PartitionIndex::from_raw(0),
                1,
                None,
                Some(bytes::Bytes::from_static(b"value")),
            ),
        )
        .unwrap_or_else(|error| panic!("admit producer record: {error:?}"));
    let (observer, operation_id, fault) = accepted.into_parts();
    assert!(operation_id.is_some());
    assert!(fault.is_ok());
    let mut data = producer
        .try_data()
        .unwrap_or_else(|error| panic!("lock producer shard: {error:?}"));
    let budget =
        ProducerTurnBudget::try_new(1, 1, 1, 1, 1).unwrap_or_else(|| panic!("nonzero turn budget"));
    data.turn(Moment::from_tick(1), budget)
        .unwrap_or_else(|error| panic!("materialize Produce batch: {error}"));
    data.turn(Moment::from_tick(1), budget)
        .unwrap_or_else(|error| panic!("arm Produce submission: {error}"));
    drop(data);
    observer
}

fn ready_limits() -> crate::producer::ProducerHostLimits {
    let policy = ProducerBatchPolicy::try_new(1, ByteCount::new(u64::MAX), 10)
        .unwrap_or_else(|_| panic!("single-record batch policy"));
    let mut limits = valid_limits();
    limits.batch_policy = policy;
    limits
}

fn driver() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver: {error}"))
}

fn shutdown(driver: &mut DriverOwner) {
    driver
        .shutdown_with_turn_limit(64, DRIVER_TURN_WAIT)
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}
