//! Coordinated registry-first restoration and observer-drop race scenarios.

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use kafka_client_core::{Deadline, PartitionIndex};

use super::{
    PendingAdmissionRegistry, PendingAttemptRestoreError, PendingAttemptRestoreOutcome,
    PendingCellError, PendingRegistryError, ProducerSendFailure, ProducerSendFailureKind,
};
use crate::producer::ProducerRecord;

#[test]
fn restoration_reinstates_exact_fifo_accounting_before_cell_pending() {
    let mut registry = PendingAdmissionRegistry::new(2, 64, 2);
    let registration = register(&mut registry, "first", 3, 40);
    let send = registration.into_send();
    let before = registry.stats();
    let attempt = take(&mut registry);
    let original = attempt
        .retained_admission_for_test()
        .unwrap_or_else(|| panic!("attempt should retain its admission"));
    let id = original.id();
    let sequence = original.sequence();

    assert!(matches!(
        attempt.restore(&mut registry),
        Ok(PendingAttemptRestoreOutcome::Restored)
    ));
    assert_eq!(registry.stats(), before);
    let restored = take(&mut registry);
    let restored_admission = restored
        .retained_admission_for_test()
        .unwrap_or_else(|| panic!("restored attempt should retain admission"));
    assert_eq!(restored_admission.id(), id);
    assert_eq!(restored_admission.sequence(), sequence);
    let local = restored
        .settle_local(ProducerSendFailure::new(
            ProducerSendFailureKind::Backpressure,
        ))
        .unwrap_or_else(|_failure| panic!("restored attempt should settle"));
    let (pending, job) = local.into_parts();
    assert_eq!(pending.into_record().topic().as_ref(), "first");
    job.dispatch_pending_notification_for_test();
    assert!(send.wait().is_err());
}

#[test]
fn failed_restore_cannot_leave_cell_pending_without_registry_entry() {
    let mut registry = PendingAdmissionRegistry::new(1, 64, 1);
    let registration = register(&mut registry, "closed", 1, 30);
    let send = registration.into_send();
    let attempt = take(&mut registry);
    let cell = attempt.cell_for_test();
    registry.begin_close();

    let failure = attempt
        .restore(&mut registry)
        .err()
        .unwrap_or_else(|| panic!("closed registry should reject restore"));
    assert_eq!(
        failure.error(),
        PendingAttemptRestoreError::Registry(PendingRegistryError::Closed)
    );
    let attempt = failure
        .into_attempt()
        .unwrap_or_else(|_failure| panic!("preflight failure should retain unchanged attempt"));
    assert_eq!(registry.stats().records, 0);
    assert!(matches!(
        cell.begin_promotion_for_test(),
        Err(PendingCellError::TransitionInProgress)
    ));

    let local = attempt
        .settle_local(ProducerSendFailure::new(ProducerSendFailureKind::Shutdown))
        .unwrap_or_else(|_failure| panic!("retained attempt should settle"));
    let (pending, job) = local.into_parts();
    assert_eq!(pending.into_record().topic().as_ref(), "closed");
    job.dispatch_pending_notification_for_test();
    assert!(send.wait().is_err());
}

#[test]
fn observer_drop_during_restore_removes_exact_reinserted_accounting() {
    let mut registry = PendingAdmissionRegistry::new(1, 64, 1);
    let registration = register(&mut registry, "raced", 4, 50);
    let send = registration.into_send();
    let attempt = take(&mut registry);
    drop(send);

    let outcome = attempt
        .restore(&mut registry)
        .unwrap_or_else(|_failure| panic!("raced restore should resolve"));
    let PendingAttemptRestoreOutcome::Abandoned(pending) = outcome else {
        panic!("observer drop should win after temporary registry insertion");
    };
    assert_eq!(pending.into_record().topic().as_ref(), "raced");
    assert_eq!(registry.stats().records, 0);
    assert_eq!(registry.stats().retained_bytes, 0);
    assert_eq!(registry.stats().notification_permits, 0);
}

#[test]
fn restore_preflight_failure_returns_unchanged_attempt_and_record() {
    let mut registry = PendingAdmissionRegistry::new(2, 64, 2);
    let held_registration = register(&mut registry, "held", 2, 20);
    let held_send = held_registration.into_send();
    let live_registration = register(&mut registry, "live", 3, 40);
    let live_id = live_registration.id();
    let live_send = live_registration.into_send();
    let attempt = take(&mut registry);
    registry.insert_fifo_index_for_test(
        attempt
            .retained_admission_for_test()
            .unwrap_or_else(|| panic!("held attempt should retain admission"))
            .sequence(),
        live_id,
    );

    let failure = attempt
        .restore(&mut registry)
        .err()
        .unwrap_or_else(|| panic!("colliding index should reject restore"));
    assert_eq!(
        failure.error(),
        PendingAttemptRestoreError::Registry(PendingRegistryError::IndexCollision)
    );
    let attempt = failure
        .into_attempt()
        .unwrap_or_else(|_failure| panic!("preflight should return the exact attempt"));
    let held = attempt
        .settle_local(ProducerSendFailure::new(ProducerSendFailureKind::Closed))
        .unwrap_or_else(|_failure| panic!("held attempt should settle"));
    let (held, held_job) = held.into_parts();
    assert_eq!(held.into_record().topic().as_ref(), "held");
    held_job.dispatch_pending_notification_for_test();
    assert!(held_send.wait().is_err());
    drop(live_send);
}

fn register(
    registry: &mut PendingAdmissionRegistry,
    topic: &str,
    value_bytes: usize,
    deadline_tick: u64,
) -> super::PendingSendRegistration {
    registry
        .register(
            record(topic, value_bytes),
            Deadline::from_tick(deadline_tick),
            Instant::now(),
        )
        .unwrap_or_else(|error| panic!("pending registration should succeed: {error:?}"))
}

fn take(registry: &mut PendingAdmissionRegistry) -> super::PendingPromotionAttempt {
    registry
        .take_next(1)
        .unwrap_or_else(|error| panic!("pending take should succeed: {error:?}"))
        .into_attempt()
        .unwrap_or_else(|| panic!("pending attempt should exist"))
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
