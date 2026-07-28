//! Automatic partition selection and explicit waiting bypass scenarios.

#![expect(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "test assertions fail immediately on invalid ownership outcomes"
)]

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use kafka_client_core::{
    ByteCount, Deadline, Moment, PartitionIndex, ProducerBatchPolicy, ProducerEffect,
    partitioning::{
        AvailablePartition, PartitionCount, TopicMetadataGeneration, TopicPartitionSource,
    },
};

use crate::{
    ProducerDeliveryError, ProducerDeliveryFailureKind, ProducerDeliveryStatus,
    clock::OperationDeadline,
};

use super::super::{
    ProducerHost, ProducerRecord,
    host_limits_test::{start, valid_limits},
};
use super::ProducerPartitioningFailure;

#[test]
fn keyed_resolution_uses_java_hash_over_logical_domain() {
    let mut host = start(partitioning_limits());
    let accepted = admit_automatic(&mut host, Some(Bytes::from_static(b"kafka")), 500);
    let request = host
        .take_partitioning_request()
        .expect("partitioning request handoff")
        .expect("keyed record needs metadata");

    assert!(
        host.apply_partitioning_view(request, &topic_view())
            .unwrap()
    );
    assert_eq!(promote_and_materialize(&mut host, 1), 1);
    drop(accepted);
}

#[test]
fn unkeyed_partition_stays_sticky_until_the_batch_seals_then_advances() {
    let mut host = start(partitioning_limits());
    let first = admit_automatic(&mut host, None, 500);
    resolve_front(&mut host);
    assert_eq!(promote_and_materialize(&mut host, 1), 0);

    let second = admit_automatic(&mut host, None, 500);
    resolve_front(&mut host);
    assert_eq!(promote_and_materialize(&mut host, 2), 1);
    drop((first, second));
}

#[test]
fn explicit_waiting_record_bypasses_topic_metadata() {
    let mut host = start(partitioning_limits());
    let accepted = host
        .try_admit_waiting(Moment::from_tick(0), deadline(500), explicit_record())
        .unwrap_or_else(|_| panic!("explicit waiting admission"));

    assert!(host.take_partitioning_request().unwrap().is_none());
    let progress = host.drive_waiting(Moment::from_tick(1), 1).unwrap();
    assert_eq!(progress.progressed, 1);
    assert!(!progress.blocked);
    assert_eq!(host.stats().store.records, 1);
    drop(accepted);
}

#[test]
fn metadata_failure_preserves_broker_code_and_not_sent_delivery() {
    let mut host = start(partitioning_limits());
    let accepted = admit_automatic(&mut host, None, 500);
    let request = host
        .take_partitioning_request()
        .unwrap()
        .expect("metadata request");
    assert!(
        host.apply_partitioning_failure(
            request,
            ProducerPartitioningFailure::MetadataUnavailable {
                broker_code: Some(-47),
            },
        )
        .unwrap()
    );
    let (_id, observer, _token) = accepted.into_parts();
    assert_not_sent(observer.wait(), ProducerDeliveryFailureKind::Routing);
}

pub(super) fn admit_automatic(
    host: &mut ProducerHost,
    key: Option<Bytes>,
    deadline_tick: u64,
) -> super::AdmittedWaiting {
    host.try_admit_waiting(
        Moment::from_tick(0),
        deadline(deadline_tick),
        automatic_record(key),
    )
    .unwrap_or_else(|_| panic!("automatic waiting admission"))
}

fn resolve_front(host: &mut ProducerHost) {
    let request = host
        .take_partitioning_request()
        .unwrap()
        .expect("automatic record metadata request");
    assert!(
        host.apply_partitioning_view(request, &topic_view())
            .unwrap()
    );
}

fn promote_and_materialize(host: &mut ProducerHost, now: u64) -> i32 {
    let progress = host.drive_waiting(Moment::from_tick(now), 1).unwrap();
    assert_eq!(progress.progressed, 1);
    super::super::test_identity::acquire_host_if_pending(host, Moment::from_tick(now));
    let effect_index = host
        .pending_effects
        .iter()
        .position(|effect| matches!(effect, ProducerEffect::MaterializeBatch { .. }))
        .expect("single-record batch must materialize");
    let ProducerEffect::MaterializeBatch { execution, .. } =
        host.pending_effects.remove(effect_index)
    else {
        unreachable!("located exact materialization effect")
    };
    let (attempt, batch) = host.store.materialization_view(execution, 1_024).unwrap();
    let partition = batch.into_parts().1;
    assert!(matches!(
        host.store.abort_materialization(attempt),
        super::super::batch_store::MaterializationAbort::Restored
    ));
    partition
}

fn automatic_record(key: Option<Bytes>) -> ProducerRecord {
    ProducerRecord::from_public(
        Arc::from("orders"),
        None,
        10,
        false,
        key,
        Some(Bytes::from_static(b"value")),
        Vec::new(),
    )
}

fn explicit_record() -> ProducerRecord {
    ProducerRecord::new(
        Arc::from("orders"),
        PartitionIndex::from_raw(2),
        10,
        None,
        Some(Bytes::from_static(b"value")),
    )
}

pub(super) fn topic_view() -> TestTopicSource {
    TestTopicSource {
        available: vec![
            AvailablePartition::new(PartitionIndex::from_raw(0), None),
            AvailablePartition::new(PartitionIndex::from_raw(1), None),
            AvailablePartition::new(PartitionIndex::from_raw(2), None),
        ],
    }
}

pub(super) struct TestTopicSource {
    available: Vec<AvailablePartition>,
}

impl TopicPartitionSource for TestTopicSource {
    fn generation(&self) -> TopicMetadataGeneration {
        TopicMetadataGeneration::from_raw(7)
    }

    fn logical_count(&self) -> PartitionCount {
        PartitionCount::try_from_raw(3).expect("nonzero partition count")
    }

    fn available_len(&self) -> usize {
        self.available.len()
    }

    fn available_at(&self, index: usize) -> Option<AvailablePartition> {
        self.available.get(index).copied()
    }
}

pub(super) fn partitioning_limits() -> super::super::ProducerHostLimits {
    let mut limits = valid_limits();
    limits.retained_bytes = 256;
    limits.completion_capacity = 3;
    limits.waiting_record_capacity = 3;
    limits.waiting_byte_capacity = 256;
    limits.record_capacity = 3;
    limits.batch_capacity = 3;
    limits.timer_capacity = 3;
    limits.batch_policy =
        ProducerBatchPolicy::try_new(1, ByteCount::new(128), 100).expect("single-record policy");
    limits
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(tick), Instant::now())
}

pub(super) fn assert_not_sent(
    result: crate::ProducerDeliveryResult,
    expected: ProducerDeliveryFailureKind,
) {
    let ProducerDeliveryError::Failed(failure) = result.expect_err("operation must fail") else {
        panic!("expected semantic producer failure")
    };
    assert_eq!(failure.kind(), expected);
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
}
