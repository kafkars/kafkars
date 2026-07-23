//! One-attempt pending settlement, routing, and first-fault scenarios.

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use kafka_client_core::{ByteCount, Deadline, Moment, PartitionIndex, ProducerBatchPolicy};

use crate::{
    ProducerSendStartFailureKind,
    clock::OperationDeadline,
    producer::{
        ProducerHostInvariantError, ProducerRecord,
        host_limits_test::{start, valid_limits},
        pending::PendingAttemptStateError,
    },
};

use super::{
    data::ProducerShardData,
    pending_fatal::PendingShardFatal,
    pending_settlement::PendingSettlementDisposition,
    promotion_error::{PendingPromotionFailure, PendingPromotionInvariant},
};

#[test]
fn idle_and_tombstone_progress_report_registry_work_honestly() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let idle = settle(&mut data, 1);
    assert_progress(idle, 0, false, PendingSettlementDisposition::Idle);

    let first = register(&mut data, "first", 100);
    let second = register(&mut data, "second", 100);
    drop(first);
    let tombstone = settle(&mut data, 2);
    assert_progress(tombstone, 1, true, PendingSettlementDisposition::Productive);
    drop(second);
}

#[test]
fn explicit_close_is_idle_without_consuming_pending_ownership() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let send = register(&mut data, "closed", 100);
    data.close_admission();
    let before = data.shard_stats();

    let progress = settle(&mut data, 1);

    assert_progress(progress, 0, true, PendingSettlementDisposition::Idle);
    assert_eq!(data.shard_stats(), before);
    drop(send);
}

#[test]
fn existing_fault_reports_faulted_without_replacing_its_exact_owner() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let pending = register(&mut data, "faulted", 100);
    let (first, first_send, _deadline) = detached_failure(11);
    data.retain_pending_fatal(PendingShardFatal::promotion(first))
        .unwrap_or_else(|_refused| panic!("first fault should install"));
    let owner_before = retained_attempt_identity(&data);
    let stats_before = data.shard_stats();

    let progress = settle(&mut data, 1);

    assert_progress(progress, 0, true, PendingSettlementDisposition::Faulted);
    assert_eq!(retained_attempt_identity(&data), owner_before);
    assert_eq!(data.shard_stats(), stats_before);
    drop((pending, first_send));
}

#[test]
fn accepted_promotion_routes_observation_and_keeps_delivery_ownership() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let send = register(&mut data, "accepted", 100);

    let progress = settle(&mut data, 1);

    assert_progress(progress, 1, false, PendingSettlementDisposition::Productive);
    let stats = data.shard_stats();
    assert_eq!(stats.pending.records, 0);
    assert_eq!(stats.pending.retained_bytes, 0);
    assert_eq!(stats.host.core_completion_slots, 1);
    assert_eq!(stats.host.pending_notification_backlog, 1);
    drop(send);
}

#[test]
fn restored_head_is_blocked_and_remaining_after_reinsertion() {
    let mut limits = valid_limits();
    limits.completion_capacity = 1;
    limits.record_capacity = 1;
    limits.batch_capacity = 1;
    limits.timer_capacity = 1;
    limits.pending_record_capacity = 1;
    limits.pending_notification_capacity = 1;
    limits.notification_capacity = 2;
    limits.batch_policy = ProducerBatchPolicy::try_new(1, ByteCount::new(64), 100)
        .unwrap_or_else(|error| panic!("test policy should validate: {error}"));
    let mut data = ProducerShardData::new(start(limits));
    let pending = register(&mut data, "pending", 100);
    let accepted = data
        .try_admit_explicit(Moment::from_tick(1), deadline(100), record("accepted"))
        .unwrap_or_else(|error| panic!("accepted capacity should fill: {error:?}"));

    let progress = settle(&mut data, 2);

    assert_progress(
        progress,
        1,
        true,
        PendingSettlementDisposition::RestoredBlocked,
    );
    assert_eq!(data.shard_stats().pending.records, 1);
    drop((pending, accepted));
}

#[test]
fn local_and_start_failures_release_admission_bytes_before_routing() {
    let mut local = ProducerShardData::new(start(valid_limits()));
    let local_send = register(&mut local, "elapsed", 5);
    let progress = settle(&mut local, 5);
    assert_progress(progress, 1, false, PendingSettlementDisposition::Productive);
    assert_eq!(local.shard_stats().aggregate_retained_bytes, 0);
    assert_eq!(local.shard_stats().host.pending_notification_backlog, 1);

    let mut start_failure = ProducerShardData::new(start(valid_limits()));
    let start_send = register(&mut start_failure, "overflow", u64::MAX);
    let progress = settle(&mut start_failure, u64::MAX);
    assert_progress(progress, 1, false, PendingSettlementDisposition::Productive);
    assert_eq!(start_failure.shard_stats().aggregate_retained_bytes, 0);
    assert_eq!(
        start_failure
            .shard_stats()
            .host
            .pending_notification_backlog,
        1
    );
    drop((local_send, start_send));
}

#[test]
fn accepted_invariant_routes_first_then_closes_with_copy_facts() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let send = register(&mut data, "accepted-fault", 100);
    let expected = ProducerHostInvariantError::MissingAdmissionIdentity;
    data.inject_post_acceptance_fault(expected);

    let progress = settle(&mut data, 1);

    assert_progress(progress, 1, false, PendingSettlementDisposition::Faulted);
    assert_eq!(data.shard_stats().host.pending_notification_backlog, 1);
    let Some(PendingShardFatal::AcceptedInvariant(facts)) = data.pending_fatal_for_test() else {
        panic!("accepted invariant should become the immutable first fault")
    };
    assert!(facts.operation_id.is_some());
    assert_eq!(facts.invariant, PendingPromotionInvariant::Host(expected));
    drop(send);
}

#[test]
fn start_invariant_releases_bytes_routes_and_then_closes() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let send = register(&mut data, "start-fault", 100);
    let expected = ProducerHostInvariantError::MissingAdmissionIdentity;
    assert_eq!(data.host.poison(expected), expected);

    let progress = settle(&mut data, 1);

    assert_progress(progress, 1, false, PendingSettlementDisposition::Faulted);
    assert_eq!(data.shard_stats().aggregate_retained_bytes, 0);
    assert_eq!(data.shard_stats().host.pending_notification_backlog, 1);
    let Some(PendingShardFatal::StartInvariant(facts)) = data.pending_fatal_for_test() else {
        panic!("start invariant should become the immutable first fault")
    };
    assert_eq!(
        facts.failure.kind(),
        ProducerSendStartFailureKind::InternalInvariant
    );
    assert_eq!(facts.invariant, PendingPromotionInvariant::Host(expected));
    drop(send);
}

#[test]
fn later_settlement_fault_is_returned_without_replacing_the_first() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let (first, first_send, _first_deadline) = detached_failure(11);
    data.retain_pending_fatal(PendingShardFatal::promotion(first))
        .unwrap_or_else(|_refused| panic!("first fault should install"));
    let (later, later_send, later_deadline) = detached_failure(22);

    let refused = match data.retain_promotion_failure_for_test(later) {
        Err(refused) => refused.into_owner(),
        Ok(_progress) => panic!("immutable first fault must return the later owner"),
    };

    let Some(PendingPromotionFailure::Detach { attempt, .. }) = refused.promotion_for_test() else {
        panic!("later exact detach owner should be returned")
    };
    assert_eq!(attempt.operation_deadline(), Some(later_deadline));
    drop((first_send, later_send, refused));
}

fn settle(
    data: &mut ProducerShardData,
    now: u64,
) -> super::pending_settlement::PendingSettlementProgress {
    data.settle_next_pending(Moment::from_tick(now))
        .unwrap_or_else(|_refused| panic!("single settlement should own the first fault"))
}

fn assert_progress(
    progress: super::pending_settlement::PendingSettlementProgress,
    inspected: usize,
    remaining: bool,
    disposition: PendingSettlementDisposition,
) {
    assert_eq!(progress.inspected(), inspected);
    assert_eq!(progress.remaining(), remaining);
    assert_eq!(progress.disposition(), disposition);
}

fn register(data: &mut ProducerShardData, topic: &str, tick: u64) -> crate::ProducerSend {
    data.register_pending(record(topic), deadline(tick))
        .unwrap_or_else(|error| panic!("pending fixture should register: {error:?}"))
        .into_send()
}

fn detached_failure(
    tick: u64,
) -> (
    PendingPromotionFailure,
    crate::ProducerSend,
    OperationDeadline,
) {
    let mut source = ProducerShardData::new(start(valid_limits()));
    let expected = deadline(tick);
    let send = source
        .register_pending(record("detached"), expected)
        .unwrap_or_else(|error| panic!("failure fixture should register: {error:?}"))
        .into_send();
    let take = source
        .pending
        .take_next(1)
        .unwrap_or_else(|error| panic!("failure fixture should claim: {error:?}"));
    let attempt = take
        .into_attempt()
        .unwrap_or_else(|| panic!("failure fixture needs one live attempt"));
    (
        PendingPromotionFailure::Detach {
            error: PendingAttemptStateError::Invariant,
            attempt: Box::new(attempt),
        },
        send,
        expected,
    )
}

fn retained_attempt_identity(data: &ProducerShardData) -> *const () {
    let Some(failure) = data
        .pending_fatal_for_test()
        .and_then(PendingShardFatal::promotion_for_test)
    else {
        panic!("first promotion fault should remain installed")
    };
    let PendingPromotionFailure::Detach { attempt, .. } = failure else {
        panic!("fixture should retain an exact detach attempt")
    };
    std::ptr::from_ref(attempt.as_ref()).cast()
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
