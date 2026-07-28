//! Prefix-batch boundary time, capacity, validation, and wake scenarios.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use kafka_client_core::{ByteCount, OperationId, ProducerBatchPolicy};

use super::super::{
    ProducerHandle, ProducerSendOptions, ProducerTrySendErrorKind,
    PublicProducerRecord as ProducerRecord,
};
use crate::{
    clock::MonotonicClock,
    producer::{
        ProducerHostLimits,
        host_limits_test::{start, valid_limits},
        ingress::{CountingWake, ProducerShardOwner},
    },
};

#[test]
fn admission_uses_one_boundary_one_lock_pass_and_one_wake() {
    let (owner, handle, wake) = setup(valid_limits());
    let capture = handle
        .capture_batch(ProducerSendOptions::new(Duration::from_secs(30)))
        .unwrap_or_else(|error| panic!("ordinary batch boundary should succeed: {error}"));
    let original_deadline = capture.absolute_deadline();

    let (accepted, rejection) = handle
        .try_send_batch_captured(
            capture,
            vec![
                record().value(Bytes::from_static(b"first")),
                record().value(Bytes::from_static(b"second")),
            ],
        )
        .into_parts();

    assert!(rejection.is_none());
    assert_eq!(accepted.len(), 2);
    assert_eq!(wake.count(), 1);
    assert_eq!(host(&owner).shard_stats().host.core_completion_slots, 2);
    for raw in [1, 2] {
        assert_eq!(
            host(&owner)
                .bound_deadline(OperationId::from_raw(raw))
                .map(crate::clock::OperationDeadline::transport),
            Some(original_deadline)
        );
    }
    for item in accepted {
        drop(item.into_observer());
    }
}

#[test]
fn first_capacity_rejection_returns_exact_suffix() {
    let mut limits = valid_limits();
    limits.completion_capacity = 1;
    limits.record_capacity = 1;
    limits.batch_policy = ProducerBatchPolicy::try_new(1, ByteCount::new(64), 100)
        .unwrap_or_else(|error| panic!("single-record batch policy should be valid: {error:?}"));
    let (owner, handle, wake) = setup(limits);
    let second = Bytes::from(vec![7, 8, 9]);
    let capture = handle
        .capture_batch(ProducerSendOptions::new(Duration::from_secs(30)))
        .unwrap_or_else(|error| panic!("ordinary batch boundary should succeed: {error}"));

    let (accepted, rejection) = handle
        .try_send_batch_captured(
            capture,
            vec![
                record().value(Bytes::from_static(b"accepted")),
                record().value(second.clone()),
                record().value(Bytes::from_static(b"untouched")),
            ],
        )
        .into_parts();
    let rejection = rejection.unwrap_or_else(|| panic!("second record must exceed capacity"));

    assert_eq!(accepted.len(), 1);
    assert_eq!(rejection.kind(), ProducerTrySendErrorKind::RecordCapacity);
    assert_eq!(rejection.records().len(), 2);
    assert_eq!(
        rejection.records()[0]
            .value_bytes()
            .map(|bytes| bytes.as_ptr()),
        Some(second.as_ptr())
    );
    assert_eq!(
        rejection.records()[1].value_bytes(),
        Some(&Bytes::from_static(b"untouched"))
    );
    assert_eq!(wake.count(), 1);
    assert_eq!(host(&owner).shard_stats().host.core_completion_slots, 1);
    for item in accepted {
        drop(item.into_observer());
    }
}

#[test]
fn invalid_batch_returns_every_record_without_partial_admission() {
    let (owner, handle, wake) = setup(valid_limits());
    let capture = handle
        .capture_batch(ProducerSendOptions::new(Duration::from_secs(30)))
        .unwrap_or_else(|error| panic!("ordinary batch boundary should succeed: {error}"));

    let (accepted, rejection) = handle
        .try_send_batch_captured(
            capture,
            vec![
                ProducerRecord::to("automatic-before"),
                ProducerRecord::to("negative-partition").partition(-1),
                record().value(Bytes::from_static(b"after")),
            ],
        )
        .into_parts();
    let rejection = rejection.unwrap_or_else(|| panic!("validation should reject the batch"));

    assert!(accepted.is_empty());
    assert_eq!(
        rejection.kind(),
        ProducerTrySendErrorKind::NegativeExplicitPartition
    );
    assert_eq!(
        rejection
            .records()
            .iter()
            .map(ProducerRecord::topic)
            .collect::<Vec<_>>(),
        vec!["automatic-before", "negative-partition", "orders"]
    );
    assert_eq!(wake.count(), 0);
    assert_eq!(host(&owner).shard_stats().host.core_completion_slots, 0);
    assert_eq!(host(&owner).shard_stats().host.waiting.records, 0);
}

#[test]
fn mixed_batch_uses_active_and_waiting_owners_under_one_lock_and_wake() {
    let (owner, handle, wake) = setup(valid_limits());
    let capture = handle
        .capture_batch(ProducerSendOptions::new(Duration::from_secs(30)))
        .unwrap_or_else(|error| panic!("ordinary batch boundary should succeed: {error}"));

    let (accepted, rejection) = handle
        .try_send_batch_captured(
            capture,
            vec![
                record().value(Bytes::from_static(b"explicit")),
                ProducerRecord::to("orders").value(Bytes::from_static(b"automatic")),
            ],
        )
        .into_parts();

    assert!(rejection.is_none());
    assert_eq!(accepted.len(), 2);
    let stats = host(&owner).shard_stats().host;
    assert_eq!(stats.store.records, 1);
    assert_eq!(stats.waiting.records, 1);
    assert_eq!(wake.count(), 1);
    for item in accepted {
        drop(item.into_observer());
    }
}

fn setup(limits: ProducerHostLimits) -> (ProducerShardOwner, ProducerHandle, Arc<CountingWake>) {
    let wake = Arc::new(CountingWake::default());
    let capacity = limits.record_capacity;
    let owner = ProducerShardOwner::new(start(limits), Arc::clone(&wake));
    let handle = ProducerHandle::from_port(
        owner.admission_port(),
        Arc::new(MonotonicClock::new()),
        capacity,
        Arc::new(()),
    );
    (owner, handle, wake)
}

fn host(
    owner: &ProducerShardOwner,
) -> std::sync::MutexGuard<'_, crate::producer::ingress::ProducerShardData> {
    owner
        .try_data()
        .unwrap_or_else(|error| panic!("test should acquire producer shard: {error:?}"))
}

fn record() -> ProducerRecord {
    ProducerRecord::to("orders")
        .partition(3)
        .key(Bytes::from_static(b"key"))
        .value(Bytes::from_static(b"value"))
}
