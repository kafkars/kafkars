//! Exact attempt and promotion ownership retained by bounded-turn failures.

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use kafka_client_core::{Deadline, PartitionIndex};

use super::{
    PendingAdmissionRegistry, PendingAttemptStateError, PendingRegistryError, ProducerSendFailure,
    ProducerSendFailureKind,
    turn_error::{PendingTakeFailure, PendingTurnFailure, PendingTurnFailureOwnership},
};
use crate::{
    clock::OperationDeadline,
    producer::{ProducerRecord, boundary::ProducerSend},
};

#[test]
fn settlement_turn_failure_retains_attempt_record_and_requested_failure() {
    let mut registry = PendingAdmissionRegistry::new(1, 64, 1);
    let send = register(&mut registry);
    let mut attempt = registry
        .take_next(1)
        .unwrap_or_else(|error| panic!("pending take should succeed: {error:?}"))
        .into_attempt()
        .unwrap_or_else(|| panic!("pending attempt should exist"));
    let record = attempt
        .detach_record()
        .unwrap_or_else(|error| panic!("record should detach: {error:?}"));
    let requested = ProducerSendFailure::new(ProducerSendFailureKind::Shutdown);
    let settlement = attempt
        .settle_local(requested)
        .err()
        .unwrap_or_else(|| panic!("detached attempt must not settle"));
    let failure = PendingTurnFailure::settlement(1, Vec::new(), settlement);

    assert_eq!(failure.error(), PendingRegistryError::ObservationState);
    assert_eq!(failure.inspected(), 1);
    let (completed, ownership) = failure.into_parts();
    assert!(completed.is_empty());
    let PendingTurnFailureOwnership::Settlement(settlement) = ownership else {
        panic!("turn failure should retain the exact settlement owner");
    };
    let (error, mut attempt, returned) = settlement.into_parts();
    assert_eq!(error, PendingAttemptStateError::RecordNotRetained);
    assert_eq!(returned, requested);
    attempt.restore_record(record).unwrap_or_else(|failure| {
        let (error, _record) = failure.into_parts();
        panic!("record should restore: {error:?}")
    });
    settle(attempt, send);
}

#[test]
fn post_claim_take_failure_retains_the_exact_promotion_owner() {
    let mut registry = PendingAdmissionRegistry::new(1, 64, 1);
    let registration = registry
        .register(
            ProducerRecord::new(
                Arc::from("claimed"),
                PartitionIndex::from_raw(0),
                1,
                None,
                Some(Bytes::from_static(b"value")),
            ),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(10), Instant::now()),
        )
        .unwrap_or_else(|error| panic!("pending registration should succeed: {error:?}"));
    let id = registration.id();
    let send = registration.into_send();
    let plan = registry
        .validate_remove(id)
        .unwrap_or_else(|error| panic!("removal should preflight: {error:?}"));
    let cell = registry.slots[id.slot()].entry.as_ref().map_or_else(
        || panic!("preflighted admission should retain its cell"),
        super::PendingAdmission::cell_for_test,
    );
    let sequence = registry.slots[id.slot()].entry.as_ref().map_or_else(
        || panic!("preflighted admission should retain its index"),
        super::PendingAdmission::sequence,
    );
    let promotion = cell
        .begin_promotion()
        .unwrap_or_else(|error| panic!("cell should claim: {error:?}"));
    assert_eq!(registry.fifo.remove(&sequence), Some(id));
    let removal = registry
        .commit_remove(plan)
        .err()
        .unwrap_or_else(|| panic!("real index corruption must fail after the claim"));
    assert_eq!(removal.error(), PendingRegistryError::CorruptIndex);
    registry.fifo.insert(sequence, id);
    let (error, plan) = removal.into_parts();
    let take = PendingTakeFailure::claimed(error, promotion, plan);
    let failure = PendingTurnFailure::take(PendingRegistryError::CorruptIndex, 1, Vec::new(), take);

    assert_eq!(failure.error(), PendingRegistryError::CorruptIndex);
    assert_eq!(failure.inspected(), 1);
    let (completed, ownership) = failure.into_parts();
    assert!(completed.is_empty());
    let PendingTurnFailureOwnership::Take(take) = ownership else {
        panic!("turn failure should retain the exact take owner");
    };
    let attempt = take.recover(&mut registry).unwrap_or_else(|failure| {
        panic!(
            "host-usable recovery should retry the retained claim: {:?}",
            failure.error()
        )
    });
    let local = attempt
        .settle_local(ProducerSendFailure::new(
            ProducerSendFailureKind::Backpressure,
        ))
        .unwrap_or_else(|_failure| panic!("recovered coordinated attempt should settle"));
    let (_admission, job) = local.into_parts();
    job.dispatch_pending_notification_for_test();
    assert!(send.wait().is_err());
}

fn register(registry: &mut PendingAdmissionRegistry) -> ProducerSend {
    registry
        .register(
            ProducerRecord::new(
                Arc::from("orders"),
                PartitionIndex::from_raw(0),
                1,
                None,
                Some(Bytes::from_static(b"value")),
            ),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(10), Instant::now()),
        )
        .unwrap_or_else(|error| panic!("pending registration should succeed: {error:?}"))
        .into_send()
}

fn settle(attempt: super::PendingPromotionAttempt, send: ProducerSend) {
    let local = attempt
        .settle_local(ProducerSendFailure::new(
            ProducerSendFailureKind::Backpressure,
        ))
        .unwrap_or_else(|_failure| panic!("restored attempt should settle"));
    let (_admission, job) = local.into_parts();
    job.dispatch_pending_notification_for_test();
    assert!(send.wait().is_err());
}
