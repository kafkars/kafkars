//! Public producer-boundary deadline, validation, and ownership scenarios.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use kafka_client_core::PartitionIndex;

use super::super::{
    ProducerHostInvariantError,
    host_limits_test::{start, valid_limits},
    ingress::{CountingWake, ProducerShardOwner},
};
use super::{
    ProducerAcceptedFault, ProducerAcceptedFaultKind, ProducerHandle, ProducerOperationId,
    ProducerSendOptions, ProducerTrySendErrorKind, PublicProducerHeader as ProducerHeader,
    PublicProducerRecord as ProducerRecord,
};
use crate::clock::MonotonicClock;

#[test]
fn explicit_try_send_captures_one_absolute_deadline_and_commits() {
    let (owner, handle, wake) = setup();
    let timeout = Duration::from_secs(30);
    let before = Instant::now()
        .checked_add(timeout)
        .unwrap_or_else(Instant::now);
    let accepted = handle.try_send(record(), ProducerSendOptions::new(timeout));
    let after = Instant::now()
        .checked_add(timeout)
        .unwrap_or_else(Instant::now);
    let Ok(accepted) = accepted else {
        panic!("valid explicit record should be accepted")
    };

    assert_eq!(
        accepted.operation_id().map(ProducerOperationId::get),
        Some(1)
    );
    assert!(accepted.absolute_deadline() >= before);
    assert!(accepted.absolute_deadline() <= after);
    assert!(accepted.fault().is_none());
    assert_eq!(host(&owner).shard_stats().host.core_completion_slots, 1);
    assert_eq!(wake.count(), 1);
    drop(accepted.into_observer());
}

#[test]
fn missing_partition_is_rejected_after_deadline_capture_with_record_intact() {
    let (_owner, handle, wake) = setup();
    let record = ProducerRecord::to("orders")
        .value(Bytes::from_static(b"value"))
        .header(ProducerHeader::null("trace"));
    let error = handle.try_send(record, ProducerSendOptions::new(Duration::from_millis(50)));
    let Err(error) = error else {
        panic!("automatic partitioning is not implemented in this slice")
    };

    assert_eq!(
        error.kind(),
        ProducerTrySendErrorKind::MissingExplicitPartition
    );
    let record = error.into_record();
    assert_eq!(record.topic(), "orders");
    assert_eq!(record.value_bytes(), Some(&Bytes::from_static(b"value")));
    assert_eq!(record.timestamp(), None);
    assert_eq!(record.headers(), &[ProducerHeader::null("trace")]);
    assert_eq!(wake.count(), 0);
}

#[test]
fn deadline_failure_precedes_record_validation() {
    let (_owner, handle, wake) = setup();
    let error = handle.try_send(
        ProducerRecord::to(""),
        ProducerSendOptions::new(Duration::new(u64::MAX, 0)),
    );
    let Err(error) = error else {
        panic!("unrepresentable deadline should fail")
    };

    assert_eq!(
        error.kind(),
        ProducerTrySendErrorKind::DeadlineUnrepresentable
    );
    assert_eq!(error.into_record().topic(), "");
    assert_eq!(wake.count(), 0);
}

#[test]
fn healthy_contention_restores_absent_timestamp_and_all_bytes() {
    let (owner, handle, wake) = setup();
    let guard = host(&owner);
    let record = record().header(ProducerHeader::new(
        "trace",
        Bytes::from_static(b"duplicate-safe"),
    ));
    let error = handle.try_send(record, ProducerSendOptions::new(Duration::from_millis(50)));
    let Err(error) = error else {
        panic!("held shard must reject without waiting")
    };

    assert_eq!(error.kind(), ProducerTrySendErrorKind::Contended);
    let record = error.into_record();
    assert_eq!(record.timestamp(), None);
    assert_eq!(record.key_bytes(), Some(&Bytes::from_static(b"key")));
    assert_eq!(record.value_bytes(), Some(&Bytes::from_static(b"value")));
    assert_eq!(record.headers().len(), 1);
    assert_eq!(wake.count(), 0);
    drop(guard);
}

#[test]
fn post_ownership_fault_remains_accepted_with_observer() {
    let (owner, handle, wake) = setup();
    host(&owner).inject_post_acceptance_fault(ProducerHostInvariantError::MissingAdmissionIdentity);
    let accepted = handle.try_send(
        record(),
        ProducerSendOptions::new(Duration::from_millis(50)),
    );
    let Ok(accepted) = accepted else {
        panic!("post-ownership invariant cannot become record-returning rejection")
    };

    assert_eq!(
        accepted.fault().map(ProducerAcceptedFault::kind),
        Some(ProducerAcceptedFaultKind::HostInvariant)
    );
    assert_eq!(
        accepted.operation_id().map(ProducerOperationId::get),
        Some(1)
    );
    assert_eq!(wake.count(), 1);
    drop(accepted.into_observer());
}

#[test]
fn captured_admission_keeps_the_original_deadline_across_adapter_work() {
    let (_owner, handle, _wake) = setup();
    let timeout = Duration::from_secs(30);
    let capture = handle.capture_send(ProducerSendOptions::new(timeout));
    let Ok(capture) = capture else {
        panic!("ordinary boundary capture should succeed")
    };
    let original_deadline = capture.absolute_deadline();

    std::thread::sleep(Duration::from_millis(2));
    let accepted = handle.try_send_captured(capture, record());
    let Ok(accepted) = accepted else {
        panic!("captured explicit record should be accepted")
    };

    assert_eq!(accepted.absolute_deadline(), original_deadline);
    drop(accepted.into_observer());
}

#[test]
fn absent_timestamp_defaults_inside_engine_and_restores_as_absent() {
    let stored = record().into_stored(PartitionIndex::from_raw(3), 1_234);
    let (_, timestamp_ms, _, _, _) = stored.into_parts();
    assert_eq!(timestamp_ms, 1_234);

    let stored = record().into_stored(PartitionIndex::from_raw(3), 1_234);
    let restored = ProducerRecord::from_stored(stored);
    assert_eq!(restored.timestamp(), None);
}

fn setup() -> (ProducerShardOwner, ProducerHandle, Arc<CountingWake>) {
    let wake = Arc::new(CountingWake::default());
    let owner = ProducerShardOwner::new(start(valid_limits()), Arc::clone(&wake));
    let handle = ProducerHandle::from_port(
        owner.admission_port(),
        Arc::new(MonotonicClock::new()),
        Arc::new(()),
    );
    (owner, handle, wake)
}

fn host(
    owner: &ProducerShardOwner,
) -> std::sync::MutexGuard<'_, crate::producer::ingress::ProducerShardData> {
    match owner.try_data() {
        Ok(data) => data,
        Err(error) => panic!("test should acquire producer shard: {error:?}"),
    }
}

fn record() -> ProducerRecord {
    ProducerRecord::to("orders")
        .partition(3)
        .key(Bytes::from_static(b"key"))
        .value(Bytes::from_static(b"value"))
}
