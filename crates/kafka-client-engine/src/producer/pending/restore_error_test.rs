//! Fault-injected restoration rollback recovery and exact ownership scenarios.

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use kafka_client_core::{Deadline, PartitionIndex};

use super::{
    PendingAdmissionRegistry, PendingAttemptRestoreError, PendingCellError, PendingRegistryError,
    ProducerSendFailure, ProducerSendFailureKind,
};
use crate::{clock::OperationDeadline, producer::ProducerRecord};

#[test]
fn double_restore_failure_retains_exact_retryable_removal_owner() {
    let mut registry = PendingAdmissionRegistry::new(1, 64, 1);
    let registration = registry
        .register(
            record("recover", 7),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(90), Instant::now()),
        )
        .unwrap_or_else(|error| panic!("pending registration should succeed: {error:?}"));
    let send = registration.into_send();
    let registered = registry.stats();
    let attempt = registry
        .take_next(1)
        .unwrap_or_else(|error| panic!("pending take should succeed: {error:?}"))
        .into_attempt()
        .unwrap_or_else(|| panic!("pending attempt should exist"));
    let admission = attempt
        .retained_admission_for_test()
        .unwrap_or_else(|| panic!("attempt should retain its admission"));
    let id = admission.id();
    let sequence = admission.sequence();
    let cell = attempt.cell_for_test();
    cell.inject_restore_failure_for_test();
    registry.inject_restore_rollback_failure_for_test();

    let failure = attempt
        .restore(&mut registry)
        .err()
        .unwrap_or_else(|| panic!("cell and rollback faults should fail restoration"));
    let expected = PendingAttemptRestoreError::Rollback {
        cell: PendingCellError::AlreadySettled,
        registry: PendingRegistryError::CorruptIndex,
    };
    assert_eq!(failure.error(), expected);
    let recovery = failure
        .into_recovery()
        .unwrap_or_else(|_failure| panic!("post-insertion failure should expose recovery owner"));
    assert_eq!(recovery.error(), expected);
    assert_eq!(registry.stats().records, 0);
    assert_eq!(registry.stats().retained_bytes, registered.retained_bytes);
    assert_eq!(registry.stats().notification_permits, 1);

    let recovery = recovery
        .recover(&mut registry)
        .err()
        .unwrap_or_else(|| panic!("corrupt exact index should retain recovery for retry"));
    assert_eq!(recovery.error(), expected);
    registry.insert_fifo_index_for_test(sequence, id);
    assert_eq!(registry.stats(), registered);

    let recovered = recovery
        .recover(&mut registry)
        .unwrap_or_else(|_recovery| panic!("repaired index should complete exact rollback"));
    let attempt = recovered
        .into_attempt()
        .unwrap_or_else(|_abandoned| panic!("cell failure should recover the promotion attempt"));
    assert_eq!(registry.stats().records, 0);
    assert_eq!(registry.stats().retained_bytes, 0);
    assert_eq!(registry.stats().notification_permits, 1);
    let local = attempt
        .settle_local(ProducerSendFailure::new(ProducerSendFailureKind::Shutdown))
        .unwrap_or_else(|_failure| panic!("recovered attempt should remain settleable"));
    let (admission, notification) = local.into_parts();
    assert_eq!(admission.id(), id);
    assert_eq!(admission.sequence(), sequence);
    assert_eq!(admission.into_record().topic().as_ref(), "recover");
    notification.dispatch_pending_notification_for_test();
    assert!(send.wait().is_err());
    assert_eq!(registry.stats().notification_permits, 0);
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
