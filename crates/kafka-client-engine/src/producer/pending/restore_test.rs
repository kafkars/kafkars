//! Promotion restoration identity, ordering, capacity, and shutdown scenarios.

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use kafka_client_core::{Deadline, PartitionIndex};

use super::{
    PendingAdmission, PendingAdmissionId, PendingAdmissionRegistry, PendingLocalFailureKind,
    PendingRegistryError, PendingRestoreOutcome,
};
use crate::{ProducerDeliveryStatus, producer::ProducerRecord};

#[test]
fn restoration_reinstates_the_exact_fifo_head_and_accounting() {
    let mut registry = PendingAdmissionRegistry::new(3, 64);
    let absolute = Instant::now();
    let first = registry
        .register(record("first", 3), deadline(40), absolute)
        .unwrap_or_else(|error| panic!("first registration failed: {error:?}"));
    let second = register(&mut registry, record("second", 4), 20);
    let before = registry.stats();
    let pending = take(&mut registry);
    let original_sequence = pending.sequence();
    let original_bytes = pending.retained_bytes();

    assert_eq!(pending.id(), first);
    assert!(matches!(
        registry.restore_front(pending),
        Ok(PendingRestoreOutcome::Restored)
    ));
    assert_eq!(registry.stats(), before);

    let restored = take(&mut registry);
    assert_eq!(restored.id(), first);
    assert_eq!(restored.sequence(), original_sequence);
    assert_eq!(restored.retained_bytes(), original_bytes);
    assert_eq!(restored.deadline(), deadline(40));
    assert_eq!(restored.absolute_instant(), absolute);
    let (topic, _, _, value, _) = restored.into_record().into_parts();
    assert_eq!(topic.as_ref(), "first");
    assert_eq!(value, Some(Bytes::from_static(b"xxx")));
    assert_eq!(take(&mut registry).id(), second);
}

#[test]
fn reused_slot_rejects_stale_restore_without_losing_either_record() {
    let mut registry = PendingAdmissionRegistry::new(2, 64);
    let absolute = Instant::now();
    let stale = registry
        .register(record("stale", 1), deadline(30), absolute)
        .unwrap_or_else(|error| panic!("stale registration failed: {error:?}"));
    let held = take(&mut registry);
    let live = register(&mut registry, record("live", 2), 50);
    let stats = registry.stats();
    let Err(failure) = registry.restore_front(held) else {
        panic!("reused slot must reject stale restore");
    };

    assert_eq!(failure.error(), PendingRegistryError::StaleGeneration);
    let (error, held) = failure.into_parts();
    assert_eq!(error, PendingRegistryError::StaleGeneration);
    assert_eq!(held.id(), stale);
    assert_eq!(held.deadline(), deadline(30));
    assert_eq!(held.absolute_instant(), absolute);
    assert_eq!(held.into_record().topic().as_ref(), "stale");
    assert_eq!(registry.stats(), stats);
    assert_eq!(take(&mut registry).id(), live);
}

#[test]
fn colliding_fifo_index_rejects_restore_before_any_mutation() {
    let mut registry = PendingAdmissionRegistry::new(2, 64);
    let held_id = register(&mut registry, record("held", 1), 30);
    let live_id = register(&mut registry, record("live", 1), 40);
    let held = take(&mut registry);
    registry.insert_fifo_index_for_test(held.sequence(), live_id);
    let stats = registry.stats();
    let Err(failure) = registry.restore_front(held) else {
        panic!("colliding FIFO index must reject restore");
    };

    assert_eq!(failure.error(), PendingRegistryError::IndexCollision);
    let (_, held) = failure.into_parts();
    assert_eq!(held.id(), held_id);
    assert_eq!(held.into_record().topic().as_ref(), "held");
    assert_eq!(registry.stats(), stats);
}

#[test]
fn unknown_slot_rejection_retains_the_complete_entry() {
    let mut registry = PendingAdmissionRegistry::new(1, 64);
    let absolute = Instant::now();
    let record = record("unknown", 2);
    let retained = record
        .retained_bytes()
        .unwrap_or_else(|error| panic!("test record size failed: {error}"));
    let id = PendingAdmissionId::new(usize::MAX, 7);
    let pending = PendingAdmission::new(id, record, deadline(70), absolute, retained, 0);
    let Err(failure) = registry.restore_front(pending) else {
        panic!("unknown slot must reject restore");
    };

    assert_eq!(failure.error(), PendingRegistryError::UnknownSlot);
    let (_, pending) = failure.into_parts();
    assert_eq!(pending.id(), id);
    assert_eq!(pending.deadline(), deadline(70));
    assert_eq!(pending.absolute_instant(), absolute);
    assert_eq!(pending.into_record().topic().as_ref(), "unknown");
    assert_eq!(registry.stats().records, 0);
    assert_eq!(registry.stats().retained_bytes, 0);
}

#[test]
fn byte_capacity_failure_retains_entry_and_existing_accounting() {
    let mut registry = PendingAdmissionRegistry::new(3, 20);
    register(&mut registry, record("a", 1), 30);
    register(&mut registry, record("b", 1), 40);
    let held = take(&mut registry);
    let other_held = take(&mut registry);
    register(&mut registry, record("c", 18), 50);
    let stats = registry.stats();
    let Err(failure) = registry.restore_front(held) else {
        panic!("byte capacity must reject restoration");
    };

    assert_eq!(failure.error(), PendingRegistryError::ByteCapacity);
    assert_eq!(failure.into_parts().1.into_record().topic().as_ref(), "a");
    assert_eq!(registry.stats(), stats);
    assert_eq!(registry.stats().records, 1);
    drop(other_held);
}

#[test]
fn close_converts_held_work_to_shutdown_instead_of_restoring() {
    let mut registry = PendingAdmissionRegistry::new(1, 64);
    let absolute = Instant::now();
    let id = registry
        .register(record("closing", 1), deadline(90), absolute)
        .unwrap_or_else(|error| panic!("registration failed: {error:?}"));
    let held = take(&mut registry);
    registry.begin_close();
    let outcome = registry
        .restore_front(held)
        .unwrap_or_else(|error| panic!("close conversion failed: {error:?}"));
    let PendingRestoreOutcome::Shutdown(failure) = outcome else {
        panic!("closed registry must settle held work");
    };

    assert_eq!(failure.kind(), PendingLocalFailureKind::Shutdown);
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
    let pending = failure.into_pending();
    assert_eq!(pending.id(), id);
    assert_eq!(pending.deadline(), deadline(90));
    assert_eq!(pending.absolute_instant(), absolute);
    assert_eq!(pending.into_record().topic().as_ref(), "closing");
    assert_eq!(registry.stats().records, 0);
    assert_eq!(registry.stats().retained_bytes, 0);
    assert!(!registry.stats().accepting);
}

fn take(registry: &mut PendingAdmissionRegistry) -> PendingAdmission {
    registry
        .take_next()
        .unwrap_or_else(|error| panic!("pending take failed: {error:?}"))
        .unwrap_or_else(|| panic!("pending entry missing"))
}

fn register(
    registry: &mut PendingAdmissionRegistry,
    record: ProducerRecord,
    deadline_tick: u64,
) -> PendingAdmissionId {
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
