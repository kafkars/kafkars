//! Critical-section evidence for the producer and tracked-driver join.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use kafka_client_core::{ByteCount, Deadline, Moment, ProducerBatchPolicy, ProducerInput};

use crate::{
    EngineConfig, ProducerDeliveryError, ProducerDeliveryFailureKind, ProducerDeliveryObserver,
    ProducerDeliveryStatus,
    clock::OperationDeadline,
    driver::{DriverOwner, TrackedProduceCalls, TrackedProducerIdentityCalls},
    producer::{
        ProducerRecord,
        host_limits_test::{start, valid_limits},
        host_turn::ProducerTurnBudget,
        ingress::{CountingWake, ProducerShardOwner},
    },
};

use super::produce::{
    admit_identity, admit_one, apply_ready, discard_routing_after_driver_shutdown,
};

const DRIVER_TURN_WAIT: Duration = Duration::from_millis(10);

#[test]
fn submitted_refresh_progresses_once_but_pending_or_rejected_refresh_does_not_spin() {
    let (producer, observer) = prepared_producer();
    let mut driver = driver();
    let mut calls = TrackedProduceCalls::with_submit_then_pending_refresh_for_test(
        kafka_client_core::BatchExecutionId::new(
            kafka_client_core::BatchId::from_raw(1),
            kafka_client_core::BatchExecutionGeneration::initial(),
        ),
        OperationDeadline::from_core_for_test(Deadline::from_tick(100)),
        ProducerInput::ExecutionUnavailable,
    );
    let mut data = producer
        .try_data()
        .unwrap_or_else(|error| panic!("lock producer shard: {error:?}"));

    assert!(
        apply_ready(&driver, &mut calls, &mut data, Moment::from_tick(2), 1)
            .unwrap_or_else(|error| panic!("submit route refresh: {error}"))
    );
    assert!(
        !apply_ready(&driver, &mut calls, &mut data, Moment::from_tick(3), 1)
            .unwrap_or_else(|error| panic!("poll pending route refresh: {error}"))
    );
    assert_eq!(calls.retained_count(), 1);

    drop((data, observer, calls));
    shutdown(&mut driver);
}

#[test]
fn immediate_driver_rejection_applies_not_sent_before_unlock() {
    let (producer, observer) = prepared_producer();
    let mut driver = driver();
    shutdown(&mut driver);
    let mut calls = TrackedProduceCalls::new(1);
    let mut routing = None;
    let mut data = producer
        .try_data()
        .unwrap_or_else(|error| panic!("lock producer shard: {error:?}"));

    let outcome = admit_one(
        &driver,
        &mut calls,
        &mut routing,
        &mut data,
        Moment::from_tick(2),
        64,
    )
    .unwrap_or_else(|error| panic!("driver rejection application: {error}"));
    assert!(outcome.did_progress());
    assert_eq!(outcome.prepared_batches(), 1);
    assert!(routing.is_none());
    assert!(calls.try_reserve().is_some());
    drop(data);

    let Err(ProducerDeliveryError::Failed(failure)) = observer.wait() else {
        panic!("driver rejection must publish one failure")
    };
    assert_eq!(failure.kind(), ProducerDeliveryFailureKind::DriverRejected);
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
}

#[test]
fn immediate_identity_rejection_applies_live_identity_failure_before_unlock() {
    let producer =
        ProducerShardOwner::new(start(ready_limits()), Arc::new(CountingWake::default()));
    let observer = admit_identity_pending(&producer);
    let mut driver = driver();
    shutdown(&mut driver);
    let mut calls = TrackedProducerIdentityCalls::new();
    let mut data = producer
        .try_data()
        .unwrap_or_else(|error| panic!("lock producer shard: {error:?}"));

    assert!(
        admit_identity(&driver, &mut calls, &mut data, Moment::from_tick(2))
            .unwrap_or_else(|error| panic!("identity rejection application: {error}"))
    );
    assert_eq!(calls.retained_count(), 0);
    assert!(!data.shard_stats().accepting);
    drop(data);

    let Err(ProducerDeliveryError::Failed(failure)) = observer.wait() else {
        panic!("live identity request rejection must publish one failure")
    };
    assert_eq!(
        failure.kind(),
        ProducerDeliveryFailureKind::ProducerIdentity
    );
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
}

#[test]
fn retained_route_lookup_preserves_the_next_prepared_owner() {
    let (producer, first) = prepared_producer();
    let mut driver = driver();
    let mut calls = TrackedProduceCalls::new(1);
    let mut routing = None;
    {
        let mut data = producer
            .try_data()
            .unwrap_or_else(|error| panic!("lock producer shard: {error:?}"));
        admit_one(
            &driver,
            &mut calls,
            &mut routing,
            &mut data,
            Moment::from_tick(2),
            64,
        )
        .unwrap_or_else(|error| panic!("first tracked admission: {error}"));
    }
    let second = admit_prepared(&producer, "payments");
    let mut data = producer
        .try_data()
        .unwrap_or_else(|error| panic!("lock producer shard: {error:?}"));
    let before = data.shard_stats().host;

    assert!(
        !admit_one(
            &driver,
            &mut calls,
            &mut routing,
            &mut data,
            Moment::from_tick(4),
            64,
        )
        .unwrap_or_else(|error| panic!("bounded admission preflight: {error}"))
        .did_progress()
    );

    let after = data.shard_stats().host;
    assert_eq!(after.prepared_batches, before.prepared_batches);
    assert_eq!(after.prepared_bytes, before.prepared_bytes);
    assert_eq!(after.submission_deadlines, before.submission_deadlines);
    assert_eq!(after.prepared_batches, 2);
    assert_eq!(after.submission_deadlines, 2);
    drop((data, first, second, calls));
    shutdown(&mut driver);
    discard_routing_after_driver_shutdown(&mut routing);
}

pub(super) fn prepared_producer() -> (ProducerShardOwner, ProducerDeliveryObserver) {
    let producer =
        ProducerShardOwner::new(start(ready_limits()), Arc::new(CountingWake::default()));
    let observer = admit_prepared(&producer, "orders");
    (producer, observer)
}

fn admit_identity_pending(producer: &ProducerShardOwner) -> ProducerDeliveryObserver {
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
    let budget = ProducerTurnBudget::try_new(1, 1, 1, 1, 1, 1)
        .unwrap_or_else(|| panic!("nonzero turn budget"));
    data.turn(Moment::from_tick(1), budget)
        .unwrap_or_else(|error| panic!("prepare identity request: {error}"));
    drop(data);
    observer
}

fn admit_prepared(producer: &ProducerShardOwner, topic: &str) -> ProducerDeliveryObserver {
    let accepted = producer
        .admission_port()
        .try_admit_explicit(
            Moment::from_tick(0),
            OperationDeadline::from_parts_for_test(
                Deadline::from_tick(500_000_000),
                Instant::now() + Duration::from_millis(500),
            ),
            ProducerRecord::new(
                Arc::from(topic),
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
    let budget = ProducerTurnBudget::try_new(1, 1, 1, 1, 1, 1)
        .unwrap_or_else(|| panic!("nonzero turn budget"));
    data.turn(Moment::from_tick(1), budget)
        .unwrap_or_else(|error| panic!("materialize Produce batch: {error}"));
    crate::producer::test_identity::acquire_shard_if_pending(&mut data, Moment::from_tick(1));
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

pub(super) fn driver() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver: {error}"))
}

pub(super) fn shutdown(driver: &mut DriverOwner) {
    driver
        .shutdown_with_turn_limit(64, DRIVER_TURN_WAIT)
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}
