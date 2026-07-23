//! Dormant shard-level FIFO promotion and ownership scenarios.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use kafka_client_core::{ByteCount, Deadline, Moment, PartitionIndex, ProducerBatchPolicy};

use crate::{
    ProducerSendStartFailureKind,
    clock::OperationDeadline,
    producer::{
        ProducerHostInvariantError, ProducerRecord,
        host_limits_test::{start, valid_limits},
        pending::ProducerSendFailureKind,
    },
};

use super::{
    data::ProducerShardData,
    promotion_error::{
        PendingPromotionFailure, PendingPromotionInvariant, PendingPromotionResolution,
    },
};

#[test]
fn later_moment_admission_binds_the_original_core_and_transport_deadline() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let transport = Instant::now()
        .checked_add(Duration::from_secs(30))
        .unwrap_or_else(|| panic!("test transport deadline should be representable"));
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(100), transport);
    let registration = data
        .register_pending(record("orders"), deadline)
        .unwrap_or_else(|error| panic!("pending record should register: {error:?}"));
    let send = registration.into_send();

    let progress = data
        .promote_next(Moment::from_tick(50))
        .unwrap_or_else(|_failure| panic!("promotion should resolve"));
    assert_eq!(progress.inspected(), 1);
    assert!(!progress.remaining());
    let Some(PendingPromotionResolution::Accepted(accepted)) = progress.into_resolution() else {
        panic!("unexpired pending record should be accepted")
    };
    let (operation_id, notification, invariant) = accepted.into_parts();
    let operation_id = operation_id.unwrap_or_else(|| panic!("accepted operation needs identity"));
    assert_eq!(invariant, None);
    assert_eq!(data.bound_deadline(operation_id), Some(deadline));

    drop(send);
    notification.dispatch_pending_notification_for_test();
    assert_eq!(data.shard_stats().pending.notification_permits, 0);
}

#[test]
fn fresh_promotion_moment_expires_without_allocating_core_ownership() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(50), Instant::now());
    let registration = data
        .register_pending(record("expired"), deadline)
        .unwrap_or_else(|error| panic!("pending record should register: {error:?}"));
    let send = registration.into_send();

    let progress = data
        .promote_next(Moment::from_tick(50))
        .unwrap_or_else(|_failure| panic!("elapsed promotion should settle"));
    let Some(PendingPromotionResolution::Local(local)) = progress.into_resolution() else {
        panic!("fresh moment must enforce the original deadline")
    };
    assert_eq!(local.kind(), ProducerSendFailureKind::DeadlineElapsed);
    let (admission, notification) = local.into_parts();
    assert_eq!(admission.operation_deadline(), deadline);
    assert_eq!(admission.into_record().topic().as_ref(), "expired");
    drop(send);
    notification.dispatch_pending_notification_for_test();

    let stats = data.shard_stats();
    assert_eq!(stats.host.store.records, 0);
    assert_eq!(stats.host.core_completion_slots, 0);
    assert_eq!(stats.pending.notification_permits, 0);
}

#[test]
fn unrepresentable_core_timing_settles_with_the_exact_start_owner() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let deadline = deadline(u64::MAX);
    let registration = data
        .register_pending(record("overflow"), deadline)
        .unwrap_or_else(|error| panic!("pending record should register: {error:?}"));
    let send = registration.into_send();

    let progress = data
        .promote_next(Moment::from_tick(u64::MAX))
        .unwrap_or_else(|_failure| panic!("core rejection should settle intact"));
    let Some(PendingPromotionResolution::Start(start)) = progress.into_resolution() else {
        panic!("deadline overflow should preserve a start-failure owner")
    };
    let (failure, invariant) = start.into_parts();
    assert_eq!(invariant, None);
    assert_eq!(
        failure.kind(),
        ProducerSendStartFailureKind::InternalInvariant
    );
    let (admission, notification) = failure.into_parts();
    assert_eq!(admission.operation_deadline(), deadline);
    assert_eq!(admission.into_record().topic().as_ref(), "overflow");
    drop(send);
    notification.dispatch_pending_notification_for_test();
    assert_eq!(data.shard_stats().pending.notification_permits, 0);
}

#[test]
fn transient_head_restores_before_any_later_fifo_entry_can_promote() {
    let mut limits = valid_limits();
    limits.completion_capacity = 1;
    limits.record_capacity = 1;
    limits.batch_capacity = 1;
    limits.timer_capacity = 1;
    limits.pending_record_capacity = 2;
    limits.pending_notification_capacity = 2;
    limits.notification_capacity = 3;
    limits.batch_policy = ProducerBatchPolicy::try_new(1, ByteCount::new(64), 100)
        .unwrap_or_else(|error| panic!("single-record policy should validate: {error}"));
    let mut data = ProducerShardData::new(start(limits));
    let first = data
        .register_pending(record("first"), deadline(100))
        .unwrap_or_else(|error| panic!("first pending record should register: {error:?}"));
    let second = data
        .register_pending(record("second"), deadline(100))
        .unwrap_or_else(|error| panic!("second pending record should register: {error:?}"));
    let accepted = data
        .try_admit_explicit(Moment::from_tick(1), deadline(100), record("accepted"))
        .unwrap_or_else(|error| panic!("accepted capacity should fill: {error:?}"));

    let progress = data
        .promote_next(Moment::from_tick(2))
        .unwrap_or_else(|_failure| panic!("transient rejection should restore"));
    assert!(matches!(
        progress.into_resolution(),
        Some(PendingPromotionResolution::Restored)
    ));
    assert_eq!(data.shard_stats().pending.records, 2);

    drop(first.into_send());
    let tombstone = data
        .promote_next(Moment::from_tick(3))
        .unwrap_or_else(|_failure| panic!("head tombstone should be removed"));
    assert_eq!(tombstone.inspected(), 1);
    assert!(tombstone.remaining());
    assert!(tombstone.into_resolution().is_none());

    let next = data
        .promote_next(Moment::from_tick(4))
        .unwrap_or_else(|_failure| panic!("second entry should retain FIFO ownership"));
    assert!(matches!(
        next.into_resolution(),
        Some(PendingPromotionResolution::Restored)
    ));
    assert_eq!(data.shard_stats().pending.records, 1);
    drop((second, accepted));
}

#[test]
fn accepted_invariant_installs_observation_before_returning_the_fault() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let registration = data
        .register_pending(record("fault"), deadline(100))
        .unwrap_or_else(|error| panic!("pending record should register: {error:?}"));
    let send = registration.into_send();
    let host_error = ProducerHostInvariantError::MissingAdmissionIdentity;
    data.inject_post_acceptance_fault(host_error);

    let progress = data
        .promote_next(Moment::from_tick(1))
        .unwrap_or_else(|_failure| panic!("accepted fault still owns observation"));
    let Some(PendingPromotionResolution::Accepted(accepted)) = progress.into_resolution() else {
        panic!("post-acceptance fault cannot restore pending ownership")
    };
    let (_operation_id, notification, invariant) = accepted.into_parts();
    assert_eq!(invariant, Some(PendingPromotionInvariant::Host(host_error)));
    assert!(!data.shard_stats().host.healthy);

    drop(send);
    notification.dispatch_pending_notification_for_test();
    assert_eq!(data.shard_stats().pending.notification_permits, 0);
}

#[test]
fn closed_shard_leaves_the_exact_pending_registry_untouched() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let registration = data
        .register_pending(record("waiting"), deadline(100))
        .unwrap_or_else(|error| panic!("pending record should register: {error:?}"));
    let before = data.shard_stats();
    data.close_admission();

    let result = data.promote_next(Moment::from_tick(1));
    assert!(matches!(result, Err(PendingPromotionFailure::Closed)));
    let after = data.shard_stats();
    assert_eq!(after.pending.records, before.pending.records);
    assert_eq!(after.pending.retained_bytes, before.pending.retained_bytes);
    assert_eq!(
        after.pending.notification_permits,
        before.pending.notification_permits
    );
    drop(registration);
}

fn record(topic: &str) -> ProducerRecord {
    ProducerRecord::new(
        Arc::from(topic),
        PartitionIndex::from_raw(0),
        10,
        None,
        Some(Bytes::from_static(b"value")),
    )
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(tick), Instant::now())
}
