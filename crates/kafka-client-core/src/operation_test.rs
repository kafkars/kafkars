//! Tests for producer operation state transitions.

use crate::{
    ByteCount, Deadline, DeliveryStatus, OperationId, ProducerCompletion, ProducerOperation,
    ProducerOperationState, TransitionError,
};

#[test]
fn waiting_operation_expires_not_sent_without_releasing_budget() {
    let mut operation = ProducerOperation::new(
        OperationId::from_raw(7),
        Deadline::from_tick(100),
        ByteCount::new(512),
    );

    let effects = operation.expire();

    assert_eq!(
        effects.map(|value| (value.completion(), value.released_bytes())),
        Ok((ProducerCompletion::Failed(DeliveryStatus::NotSent), None,))
    );
    assert_eq!(operation.state(), ProducerOperationState::Completed);
}

#[test]
fn admitted_operation_expires_not_sent_and_releases_budget() {
    let bytes = ByteCount::new(1_024);
    let mut operation =
        ProducerOperation::new(OperationId::from_raw(8), Deadline::from_tick(200), bytes);

    assert_eq!(operation.admit(), Ok(()));
    let effects = operation.expire();

    assert_eq!(
        effects.map(|value| (value.completion(), value.released_bytes())),
        Ok((
            ProducerCompletion::Failed(DeliveryStatus::NotSent),
            Some(bytes),
        ))
    );
}

#[test]
fn submitted_operation_waits_for_driver_delivery_certainty() {
    let bytes = ByteCount::new(2_048);
    let mut operation =
        ProducerOperation::new(OperationId::from_raw(9), Deadline::from_tick(300), bytes);

    assert_eq!(operation.admit(), Ok(()));
    assert_eq!(operation.mark_submitted(), Ok(()));
    assert_eq!(operation.expire(), Err(TransitionError::InvalidState));
    let effects = operation.complete_failed(DeliveryStatus::PossiblySent);

    assert_eq!(
        effects.map(|value| (value.completion(), value.released_bytes())),
        Ok((
            ProducerCompletion::Failed(DeliveryStatus::PossiblySent),
            Some(bytes),
        ))
    );
}

#[test]
fn producer_terminal_transition_is_exactly_once() {
    let mut operation = ProducerOperation::new(
        OperationId::from_raw(10),
        Deadline::from_tick(400),
        ByteCount::new(64),
    );

    assert_eq!(operation.reject().map(|_| ()), Ok(()));
    assert_eq!(operation.expire(), Err(TransitionError::AlreadyCompleted));
    assert_eq!(operation.reject(), Err(TransitionError::AlreadyCompleted));
}
