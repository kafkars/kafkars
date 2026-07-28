//! Conservative observer recovery after accepted waiting ownership is damaged.

#![expect(
    clippy::expect_used,
    reason = "test fixture inspects the expected error variant"
)]

use std::time::Instant;

use kafka_client_core::{Deadline, Moment, OperationId};

use crate::{
    ProducerDeliveryError, ProducerDeliveryFailureKind, ProducerDeliveryStatus,
    clock::OperationDeadline,
};

use crate::producer::{
    ProducerHostInvariantError,
    admission_test::record,
    host_limits_test::{start, valid_limits},
};

#[test]
fn waiting_settlement_damage_still_publishes_execution_unavailable() {
    let mut host = start(valid_limits());
    let waiting = host
        .try_admit_waiting(
            Moment::from_tick(0),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(100), Instant::now()),
            record("waiting"),
        )
        .unwrap_or_else(|_| panic!("waiting admission should succeed"));
    let operation_id = OperationId::from_raw(1);
    host.bindings
        .remove(operation_id)
        .unwrap_or_else(|error| panic!("waiting binding setup failed: {error}"));

    let error = host
        .execution_unavailable(Moment::from_tick(1))
        .expect_err("damaged waiting settlement must remain reportable");
    assert!(error.to_string().contains("owns no completion binding"));
    let (_waiter_id, observer, _token) = waiting.into_parts();
    assert_fallback(observer, &host);
}

#[test]
fn missing_admission_identity_path_keeps_reserved_observer_recoverable() {
    let mut host = start(valid_limits());
    let waiting = host
        .try_admit_waiting(
            Moment::from_tick(0),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(100), Instant::now()),
            record("waiting"),
        )
        .unwrap_or_else(|_| panic!("waiting admission should succeed"));
    let (waiter_id, observer, _token) = waiting.into_parts();
    let entry = host
        .waiting
        .remove(waiter_id)
        .unwrap_or_else(|| panic!("waiting entry setup"));
    assert!(host.waiting_policy.remove(waiter_id).is_some());
    host.store
        .release_waiting_topic(entry.topic_id)
        .unwrap_or_else(|error| panic!("waiting topic setup failed: {error}"));
    drop(entry.record);
    host.bindings
        .remove(OperationId::from_raw(1))
        .unwrap_or_else(|error| panic!("waiting binding setup failed: {error}"));
    host.poison(ProducerHostInvariantError::MissingAdmissionIdentity);

    let error = host
        .execution_unavailable(Moment::from_tick(1))
        .expect_err("missing identity must remain reportable");
    assert!(error.to_string().contains("operation identity"));
    assert_fallback(observer, &host);
}

#[test]
fn missing_waiting_entry_cannot_stall_execution_stop_fallback() {
    let mut host = start(valid_limits());
    let waiting = host
        .try_admit_waiting(
            Moment::from_tick(0),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(100), Instant::now()),
            record("waiting"),
        )
        .unwrap_or_else(|_| panic!("waiting admission should succeed"));
    let (waiter_id, observer, _token) = waiting.into_parts();
    let removed = host
        .waiting
        .remove(waiter_id)
        .unwrap_or_else(|| panic!("waiting entry setup"));
    drop(removed);
    assert_eq!(host.waiting_policy.len(), 1);

    let error = host
        .execution_unavailable(Moment::from_tick(1))
        .expect_err("missing waiting entry must remain reportable");
    assert!(error.to_string().contains("waiting policy"));
    assert_fallback(observer, &host);
}

fn assert_fallback(observer: crate::ProducerDeliveryObserver, host: &super::super::ProducerHost) {
    let Err(ProducerDeliveryError::Failed(failure)) = observer.wait() else {
        panic!("fallback must publish a semantic producer failure")
    };
    assert_eq!(
        failure.kind(),
        ProducerDeliveryFailureKind::ExecutionUnavailable
    );
    assert_eq!(
        failure.delivery_status(),
        ProducerDeliveryStatus::PossiblySent
    );
    let stats = host.stats();
    assert_eq!(stats.waiting.records, 0);
    assert_eq!(stats.waiting.bytes, kafka_client_core::ByteCount::new(0));
    assert_eq!(stats.waiting.terminal_bindings, 0);
    assert_eq!(host.waiting_policy.len(), 0);
    assert_eq!(stats.core_completion_slots, 0);
    assert_eq!(stats.completion_bindings, 0);
    assert_eq!(host.unsettled_completions(), 0);
    assert!(host.terminal_resources_empty());
}
