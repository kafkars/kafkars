//! Count, byte, ordering, deadline, cancellation, and shutdown scenarios.

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use kafka_client_core::{Deadline, Moment, PartitionIndex};

use super::{
    PendingAdmissionRegistry, PendingAdmissionRejectionReason, PendingLocalFailureKind,
    PendingRegistryError,
};
use crate::{ProducerDeliveryStatus, producer::ProducerRecord};

#[test]
fn count_and_byte_bounds_return_the_exact_record() {
    let mut count_limited = PendingAdmissionRegistry::new(1, 64);
    let first = register(&mut count_limited, record("first", 1), 30);
    let Err(rejected) = count_limited.register(record("second", 1), deadline(40), Instant::now())
    else {
        panic!("count capacity should reject");
    };
    assert_eq!(
        rejected.reason(),
        PendingAdmissionRejectionReason::CountCapacity
    );
    assert_eq!(rejected.into_record().topic().as_ref(), "second");
    assert_eq!(count_limited.stats().records, 1);
    assert_eq!(
        count_limited
            .cancel(first)
            .unwrap_or_else(|error| panic!("first cancellation failed: {error:?}"))
            .into_record()
            .topic()
            .as_ref(),
        "first"
    );

    let mut byte_limited = PendingAdmissionRegistry::new(2, 6);
    let Err(rejected) = byte_limited.register(record("orders", 1), deadline(50), Instant::now())
    else {
        panic!("byte capacity should reject");
    };
    assert_eq!(
        rejected.reason(),
        PendingAdmissionRejectionReason::ByteCapacity
    );
    assert_eq!(rejected.into_record().topic().as_ref(), "orders");
    assert_eq!(byte_limited.stats().retained_bytes, 0);
}

#[test]
fn promotion_is_fifo_and_releases_exact_accounting() {
    let mut registry = PendingAdmissionRegistry::new(3, 64);
    register(&mut registry, record("one", 1), 90);
    register(&mut registry, record("two", 2), 20);
    register(&mut registry, record("three", 3), 50);
    assert_eq!(registry.next_deadline(), Some(deadline(20)));

    for expected in ["one", "two", "three"] {
        let pending = registry
            .take_next()
            .unwrap_or_else(|error| panic!("FIFO take failed: {error:?}"))
            .unwrap_or_else(|| panic!("missing pending record {expected}"));
        assert_eq!(pending.into_record().topic().as_ref(), expected);
    }
    assert_eq!(
        registry.stats(),
        super::PendingAdmissionStats {
            records: 0,
            retained_bytes: 0,
            accepting: true,
        }
    );
}

#[test]
fn pending_entry_preserves_both_absolute_deadline_representations() {
    let mut registry = PendingAdmissionRegistry::new(1, 64);
    let absolute = Instant::now();
    let id = registry
        .register(record("orders", 1), deadline(44), absolute)
        .unwrap_or_else(|error| panic!("registration failed: {error:?}"));
    let pending = registry
        .take_next()
        .unwrap_or_else(|error| panic!("take failed: {error:?}"))
        .unwrap_or_else(|| panic!("pending entry missing"));

    assert_eq!(pending.absolute_instant(), absolute);
    let (actual_id, record, actual_deadline, actual_absolute) = pending.into_parts();
    assert_eq!(actual_id, id);
    assert_eq!(record.topic().as_ref(), "orders");
    assert_eq!(actual_deadline, deadline(44));
    assert_eq!(actual_absolute, absolute);
}

#[test]
fn stale_generation_cannot_cancel_a_reused_slot() {
    let mut registry = PendingAdmissionRegistry::new(1, 64);
    let stale = register(&mut registry, record("old", 1), 10);
    let old = registry
        .cancel(stale)
        .unwrap_or_else(|error| panic!("old cancel failed: {error:?}"));
    drop(old);
    let live = register(&mut registry, record("live", 1), 20);

    assert_ne!(stale, live);
    let Err(error) = registry.cancel(stale) else {
        panic!("stale generation should not cancel live work");
    };
    assert_eq!(error, PendingRegistryError::StaleGeneration);
    assert_eq!(
        registry
            .cancel(live)
            .unwrap_or_else(|error| panic!("live cancel failed: {error:?}"))
            .into_record()
            .topic()
            .as_ref(),
        "live"
    );
}

#[test]
fn cancellation_and_promotion_have_one_linear_winner() {
    let mut cancelled_first = PendingAdmissionRegistry::new(1, 64);
    let id = register(&mut cancelled_first, record("cancelled", 1), 10);
    let returned = cancelled_first
        .cancel(id)
        .unwrap_or_else(|error| panic!("cancel should win: {error:?}"));
    assert_eq!(returned.id(), id);
    assert!(
        cancelled_first
            .take_next()
            .unwrap_or_else(|error| panic!("empty take failed: {error:?}"))
            .is_none()
    );

    let mut promoted_first = PendingAdmissionRegistry::new(1, 64);
    let id = register(&mut promoted_first, record("promoted", 1), 10);
    let promoted = promoted_first
        .take_next()
        .unwrap_or_else(|error| panic!("promotion failed: {error:?}"))
        .unwrap_or_else(|| panic!("promotion should own the record"));
    assert_eq!(promoted.id(), id);
    let Err(error) = promoted_first.cancel(id) else {
        panic!("promoted ownership should win over cancellation");
    };
    assert_eq!(error, PendingRegistryError::StaleGeneration);
}

#[test]
fn deadline_expiry_is_ordered_bounded_and_not_sent() {
    let mut registry = PendingAdmissionRegistry::new(4, 128);
    register(&mut registry, record("late", 1), 90);
    register(&mut registry, record("first-due", 1), 10);
    register(&mut registry, record("second-due", 1), 20);
    register(&mut registry, record("future", 1), 60);

    let expired = registry
        .expire_due(Moment::from_tick(50), 1)
        .unwrap_or_else(|error| panic!("expiry failed: {error:?}"));
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].kind(), PendingLocalFailureKind::DeadlineElapsed);
    assert_eq!(
        expired[0].delivery_status(),
        ProducerDeliveryStatus::NotSent
    );
    assert_eq!(
        expired
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("expired result missing"))
            .into_pending()
            .into_record()
            .topic()
            .as_ref(),
        "first-due"
    );
    assert_eq!(registry.next_deadline(), Some(deadline(20)));

    let expired = registry
        .expire_due(Moment::from_tick(50), 8)
        .unwrap_or_else(|error| panic!("second expiry failed: {error:?}"));
    assert_eq!(expired.len(), 1);
    assert_eq!(
        expired
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("second result missing"))
            .into_pending()
            .into_record()
            .topic()
            .as_ref(),
        "second-due"
    );
    assert_eq!(registry.next_deadline(), Some(deadline(60)));
}

#[test]
fn shutdown_closes_registration_and_drains_in_fifo_budgets() {
    let mut registry = PendingAdmissionRegistry::new(3, 64);
    register(&mut registry, record("one", 1), 10);
    register(&mut registry, record("two", 1), 20);
    let Err(error) = registry.drain_closed(1) else {
        panic!("an open registry must not shutdown-drain");
    };
    assert_eq!(error, PendingRegistryError::StillOpen);

    registry.begin_close();
    let Err(rejected) = registry.register(record("late", 1), deadline(30), Instant::now()) else {
        panic!("closing must reject new pending records");
    };
    assert_eq!(rejected.reason(), PendingAdmissionRejectionReason::Closed);
    assert_eq!(rejected.into_record().topic().as_ref(), "late");

    let first = registry
        .drain_closed(1)
        .unwrap_or_else(|error| panic!("first drain failed: {error:?}"));
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].kind(), PendingLocalFailureKind::Shutdown);
    assert_eq!(registry.stats().records, 1);
    let second = registry
        .drain_closed(4)
        .unwrap_or_else(|error| panic!("second drain failed: {error:?}"));
    assert_eq!(second.len(), 1);
    assert_eq!(registry.stats().records, 0);
    assert!(!registry.stats().accepting);
}

#[test]
fn exhausted_slot_generation_retires_that_capacity() {
    let mut registry = PendingAdmissionRegistry::new(1, 64);
    registry
        .set_vacant_generation_for_test(0, u64::MAX)
        .unwrap_or_else(|error| panic!("generation setup failed: {error:?}"));
    let Err(rejected) = registry.register(record("orders", 1), deadline(10), Instant::now()) else {
        panic!("retired generation must reject registration");
    };
    assert_eq!(
        rejected.reason(),
        PendingAdmissionRejectionReason::IdentityExhausted
    );
    assert_eq!(rejected.into_record().topic().as_ref(), "orders");
}

fn register(
    registry: &mut PendingAdmissionRegistry,
    record: ProducerRecord,
    deadline_tick: u64,
) -> super::PendingAdmissionId {
    registry
        .register(record, deadline(deadline_tick), Instant::now())
        .unwrap_or_else(|error| panic!("pending registration failed: {error:?}"))
}

fn deadline(tick: u64) -> Deadline {
    Deadline::from_tick(tick)
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
