//! Exact typed failure ownership through pending-attempt settlement.

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use kafka_client_core::{Deadline, PartitionIndex};

use super::{
    PendingAdmissionRegistry, PendingAttemptStateError, PendingSendRegistration,
    ProducerSendFailure, ProducerSendFailureKind,
};
use crate::{
    ProducerSendError, ProducerSendStartFailure, ProducerSendStartFailureKind,
    clock::OperationDeadline,
    producer::{
        ProducerRecord,
        pending::test_support::{CountingWake, poll_send},
    },
};

#[test]
fn start_failure_survives_attempt_and_cell_without_becoming_backpressure() {
    let mut registry = PendingAdmissionRegistry::new(1, 64, 1);
    let registration = register(&mut registry, "orders");
    let mut send = registration.into_send();
    let attempt = registry
        .take_next(1)
        .unwrap_or_else(|error| panic!("pending take should succeed: {error:?}"))
        .into_attempt()
        .unwrap_or_else(|| panic!("live pending attempt should exist"));
    let failure = ProducerSendStartFailure::new(ProducerSendStartFailureKind::InternalInvariant);
    let settled = attempt
        .settle_start(failure)
        .unwrap_or_else(|_error| panic!("retained attempt should settle"));
    assert_eq!(settled.failure(), failure);
    assert_eq!(
        settled.kind(),
        ProducerSendStartFailureKind::InternalInvariant
    );
    let (admission, job) = settled.into_parts();
    let record: ProducerRecord = admission.into_record();
    assert_eq!(record.topic().as_ref(), "orders");
    job.dispatch_pending_notification_for_test();
    assert_eq!(
        poll_send(&mut send, CountingWake::new()),
        std::task::Poll::Ready(Err(ProducerSendError::Start(failure)))
    );
}

#[test]
fn rejected_start_settlement_returns_the_exact_typed_failure() {
    let mut registry = PendingAdmissionRegistry::new(1, 64, 1);
    let registration = register(&mut registry, "orders");
    let send = registration.into_send();
    let mut attempt = registry
        .take_next(1)
        .unwrap_or_else(|error| panic!("pending take should succeed: {error:?}"))
        .into_attempt()
        .unwrap_or_else(|| panic!("live pending attempt should exist"));
    let record = attempt
        .detach_record()
        .unwrap_or_else(|error| panic!("record should detach: {error:?}"));
    let failure =
        ProducerSendStartFailure::new(ProducerSendStartFailureKind::RecordSizeUnrepresentable);
    let rejected = attempt
        .settle_start(failure)
        .err()
        .unwrap_or_else(|| panic!("detached record must not settle"));
    let (error, mut attempt, returned) = rejected.into_parts();
    assert_eq!(error, PendingAttemptStateError::RecordNotRetained);
    assert_eq!(returned, failure);
    attempt.restore_record(record).unwrap_or_else(|failure| {
        let (error, _record) = failure.into_parts();
        panic!("record should restore after typed rejection: {error:?}")
    });
    let local = attempt
        .settle_local(ProducerSendFailure::new(ProducerSendFailureKind::Closed))
        .unwrap_or_else(|_failure| panic!("restored attempt should settle"));
    let (_admission, job) = local.into_parts();
    job.dispatch_pending_notification_for_test();
    assert!(send.wait().is_err());
}

fn register(registry: &mut PendingAdmissionRegistry, topic: &str) -> PendingSendRegistration {
    registry
        .register(
            ProducerRecord::new(
                Arc::from(topic),
                PartitionIndex::from_raw(0),
                1,
                None,
                Some(Bytes::from_static(b"value")),
            ),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(40), Instant::now()),
        )
        .unwrap_or_else(|error| panic!("pending registration should succeed: {error:?}"))
}
