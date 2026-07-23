//! Count, byte, ordering, deadline, cancellation, and shutdown scenarios.

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use kafka_client_core::{Deadline, Moment, PartitionIndex};

use super::{
    PendingAdmissionRegistry, PendingAdmissionRejectionReason, PendingRegistryError,
    ProducerSendFailureKind,
};
use crate::{ProducerDeliveryStatus, clock::OperationDeadline, producer::ProducerRecord};

#[test]
fn count_and_byte_bounds_return_the_exact_record() {
    let mut count_limited = PendingAdmissionRegistry::new(1, 64, 1);
    let first = register(&mut count_limited, record("first", 1), 30);
    let first_id = first.id();
    let Err(rejected) = count_limited.register(record("second", 1), operation_deadline(40)) else {
        panic!("count capacity should reject");
    };
    assert_eq!(
        rejected.reason(),
        PendingAdmissionRejectionReason::CountCapacity
    );
    assert_eq!(rejected.into_record().topic().as_ref(), "second");
    drop(first.into_send());
    assert_eq!(
        count_limited
            .cancel(first_id)
            .unwrap_or_else(|error| panic!("first cancellation failed: {error:?}"))
            .into_record()
            .topic()
            .as_ref(),
        "first"
    );

    let mut byte_limited = PendingAdmissionRegistry::new(2, 6, 2);
    let Err(rejected) = byte_limited.register(record("orders", 1), operation_deadline(50)) else {
        panic!("byte capacity should reject");
    };
    assert_eq!(
        rejected.reason(),
        PendingAdmissionRejectionReason::ByteCapacity
    );
    assert_eq!(rejected.into_record().topic().as_ref(), "orders");
}

#[test]
fn promotion_is_fifo_and_releases_exact_accounting() {
    let mut registry = PendingAdmissionRegistry::new(3, 64, 3);
    let sends = [
        register(&mut registry, record("one", 1), 90).into_send(),
        register(&mut registry, record("two", 2), 20).into_send(),
        register(&mut registry, record("three", 3), 50).into_send(),
    ];
    assert_eq!(registry.next_deadline(), Some(deadline(20)));

    for (expected, send) in ["one", "two", "three"].into_iter().zip(sends) {
        let attempt = take(&mut registry);
        assert_eq!(
            attempt
                .retained_admission_for_test()
                .unwrap_or_else(|| panic!("attempt should retain admission"))
                .topic_for_test(),
            expected
        );
        settle(attempt, send, ProducerSendFailureKind::Backpressure);
    }
    assert_eq!(registry.stats().records, 0);
    assert_eq!(registry.stats().retained_bytes, 0);
    assert_eq!(registry.stats().notification_permits, 0);
}

#[test]
fn pending_entry_preserves_both_absolute_deadline_representations() {
    let mut registry = PendingAdmissionRegistry::new(1, 64, 1);
    let absolute = Instant::now();
    let registration = registry
        .register(
            record("orders", 1),
            OperationDeadline::from_parts_for_test(deadline(44), absolute),
        )
        .unwrap_or_else(|error| panic!("registration failed: {error:?}"));
    let id = registration.id();
    let send = registration.into_send();
    let attempt = take(&mut registry);
    let pending = attempt
        .retained_admission_for_test()
        .unwrap_or_else(|| panic!("attempt should retain admission"));
    assert_eq!(pending.id(), id);
    assert_eq!(pending.deadline(), deadline(44));
    assert_eq!(pending.operation_deadline().transport(), absolute);
    settle(attempt, send, ProducerSendFailureKind::Closed);
}

#[test]
fn stale_generation_cannot_cancel_a_reused_slot() {
    let mut registry = PendingAdmissionRegistry::new(1, 64, 1);
    let stale = register(&mut registry, record("old", 1), 10);
    let stale_id = stale.id();
    drop(stale.into_send());
    drop(
        registry
            .cancel(stale_id)
            .unwrap_or_else(|error| panic!("old cancel failed: {error:?}")),
    );
    let live = register(&mut registry, record("live", 1), 20);
    let live_id = live.id();
    assert_ne!(stale_id, live_id);
    assert_eq!(
        registry
            .cancel(stale_id)
            .err()
            .unwrap_or_else(|| panic!("stale cancel should fail")),
        PendingRegistryError::StaleGeneration
    );
    drop(live.into_send());
    assert_eq!(
        registry
            .cancel(live_id)
            .unwrap_or_else(|error| panic!("live cancel failed: {error:?}"))
            .into_record()
            .topic()
            .as_ref(),
        "live"
    );
}

#[test]
fn deadline_expiry_is_ordered_bounded_and_not_sent() {
    let mut registry = PendingAdmissionRegistry::new(4, 128, 4);
    let late = register(&mut registry, record("late", 1), 90).into_send();
    let first = register(&mut registry, record("first-due", 1), 10).into_send();
    let second = register(&mut registry, record("second-due", 1), 20).into_send();
    let future = register(&mut registry, record("future", 1), 60).into_send();

    let first_expired = registry
        .expire_due(Moment::from_tick(50), 1)
        .unwrap_or_else(|error| panic!("expiry failed: {error:?}"));
    assert_eq!(first_expired.len(), 1);
    assert_eq!(first_expired.inspected(), 1);
    assert!(first_expired.remaining());
    assert_expired(first_expired.into_failures(), first, "first-due");
    assert_eq!(registry.next_deadline(), Some(deadline(20)));

    let second_expired = registry
        .expire_due(Moment::from_tick(50), 8)
        .unwrap_or_else(|error| panic!("second expiry failed: {error:?}"));
    assert_eq!(second_expired.len(), 1);
    assert_eq!(second_expired.inspected(), 1);
    assert!(!second_expired.remaining());
    assert_expired(second_expired.into_failures(), second, "second-due");
    assert_eq!(registry.next_deadline(), Some(deadline(60)));
    drop(late);
    drop(future);
}

#[test]
fn shutdown_closes_registration_and_drains_in_fifo_budgets() {
    let mut registry = PendingAdmissionRegistry::new(3, 64, 3);
    let first_send = register(&mut registry, record("one", 1), 10).into_send();
    let second_send = register(&mut registry, record("two", 1), 20).into_send();
    let open = registry
        .drain_closed(1)
        .err()
        .unwrap_or_else(|| panic!("open drain should fail"));
    assert_eq!(open.error(), PendingRegistryError::StillOpen);
    assert_eq!(open.inspected(), 0);
    registry.begin_close();
    let Err(rejected) = registry.register(record("late", 1), operation_deadline(30)) else {
        panic!("closing must reject new pending records");
    };
    assert_eq!(rejected.reason(), PendingAdmissionRejectionReason::Closed);

    let first = registry
        .drain_closed(1)
        .unwrap_or_else(|error| panic!("first drain failed: {error:?}"));
    assert_eq!(first.inspected(), 1);
    assert!(first.remaining());
    assert_shutdown(first.into_failures(), first_send, "one");
    assert_eq!(registry.stats().records, 1);
    let second = registry
        .drain_closed(4)
        .unwrap_or_else(|error| panic!("second drain failed: {error:?}"));
    assert_eq!(second.inspected(), 1);
    assert!(!second.remaining());
    assert_shutdown(second.into_failures(), second_send, "two");
    assert_eq!(registry.stats().records, 0);
    assert!(!registry.stats().accepting);
}

#[test]
fn exhausted_slot_generation_retires_capacity_without_losing_permit() {
    let mut registry = PendingAdmissionRegistry::new(1, 64, 1);
    registry
        .set_vacant_generation_for_test(0, u64::MAX)
        .unwrap_or_else(|error| panic!("generation setup failed: {error:?}"));
    let Err(rejected) = registry.register(record("orders", 1), operation_deadline(10)) else {
        panic!("retired generation must reject registration");
    };
    assert_eq!(
        rejected.reason(),
        PendingAdmissionRejectionReason::IdentityExhausted
    );
    assert_eq!(rejected.into_record().topic().as_ref(), "orders");
    assert_eq!(registry.stats().notification_permits, 0);
}

fn register(
    registry: &mut PendingAdmissionRegistry,
    record: ProducerRecord,
    deadline_tick: u64,
) -> super::PendingSendRegistration {
    registry
        .register(record, operation_deadline(deadline_tick))
        .unwrap_or_else(|error| panic!("pending registration failed: {error:?}"))
}

fn take(registry: &mut PendingAdmissionRegistry) -> super::PendingPromotionAttempt {
    registry
        .take_next(1)
        .unwrap_or_else(|error| panic!("FIFO take failed: {error:?}"))
        .into_attempt()
        .unwrap_or_else(|| panic!("pending attempt missing"))
}

fn settle(
    attempt: super::PendingPromotionAttempt,
    send: crate::ProducerSend,
    kind: ProducerSendFailureKind,
) {
    let local = attempt
        .settle_local(super::ProducerSendFailure::new(kind))
        .unwrap_or_else(|_failure| panic!("attempt should settle"));
    let (_pending, job) = local.into_parts();
    job.dispatch_pending_notification_for_test();
    assert!(send.wait().is_err());
}

fn assert_expired(
    failures: Vec<super::PendingLocalFailure>,
    send: crate::ProducerSend,
    topic: &str,
) {
    let failure = failures
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("expired result missing"));
    assert_eq!(failure.kind(), ProducerSendFailureKind::DeadlineElapsed);
    assert_eq!(
        failure.failure().delivery_status(),
        ProducerDeliveryStatus::NotSent
    );
    let (pending, job) = failure.into_parts();
    assert_eq!(pending.into_record().topic().as_ref(), topic);
    job.dispatch_pending_notification_for_test();
    assert!(send.wait().is_err());
}

fn assert_shutdown(
    failures: Vec<super::PendingLocalFailure>,
    send: crate::ProducerSend,
    topic: &str,
) {
    let failure = failures
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("shutdown result missing"));
    assert_eq!(failure.kind(), ProducerSendFailureKind::Shutdown);
    let (pending, job) = failure.into_parts();
    assert_eq!(pending.into_record().topic().as_ref(), topic);
    job.dispatch_pending_notification_for_test();
    assert!(send.wait().is_err());
}

fn deadline(tick: u64) -> Deadline {
    Deadline::from_tick(tick)
}

fn operation_deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(deadline(tick), Instant::now())
}

fn record(topic: &str, value_bytes: usize) -> ProducerRecord {
    ProducerRecord::new(
        Arc::from(topic),
        PartitionIndex::from_raw(0),
        1,
        None,
        Some(Bytes::from(vec![b'x'; value_bytes])),
    )
}
