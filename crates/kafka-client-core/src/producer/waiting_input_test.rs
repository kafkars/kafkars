//! Public cancellation, deadline, and execution-stop facts for waiting operations.

use crate::{
    ByteCount, Deadline, DeliveryStatus, Moment, ProducerCancellationOutcome, ProducerCompletion,
    ProducerEffect, ProducerFailureKind, ProducerInput, ProducerMachine, ProducerMachineError,
    TransitionError,
};

#[test]
fn cancellation_input_settles_waiting_before_its_flush() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    let operation_id = admit_waiting(&mut producer);
    request_flush(&mut producer);

    let terminal = producer
        .apply(ProducerInput::CancelRequested { operation_id })
        .unwrap_or_else(|error| panic!("waiting cancellation failed: {error}"));

    assert_eq!(
        terminal.cancellation_outcome(),
        Some(ProducerCancellationOutcome::CancelledNotSent)
    );
    assert_record_then_flush(
        terminal.effects(),
        operation_id,
        ProducerFailureKind::Cancelled,
    );
}

#[test]
fn deadline_input_validates_original_waiting_deadline_then_settles() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    let operation_id = admit_waiting(&mut producer);
    request_flush(&mut producer);

    assert_eq!(
        producer.apply(ProducerInput::DeadlineElapsed {
            operation_id,
            now: Moment::from_tick(9),
        }),
        Err(ProducerMachineError::Transition(
            TransitionError::DeadlineNotElapsed,
        ))
    );
    let terminal = producer
        .apply(ProducerInput::DeadlineElapsed {
            operation_id,
            now: Moment::from_tick(10),
        })
        .unwrap_or_else(|error| panic!("waiting deadline failed: {error}"));
    assert_record_then_flush(
        terminal.effects(),
        operation_id,
        ProducerFailureKind::DeadlineElapsed,
    );
}

#[test]
fn execution_stop_settles_waiting_without_payload_release_before_flush() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    let operation_id = admit_waiting(&mut producer);
    request_flush(&mut producer);

    let terminal = producer
        .apply(ProducerInput::ExecutionUnavailable)
        .unwrap_or_else(|error| panic!("waiting execution stop failed: {error}"));

    assert_record_then_flush(
        terminal.effects(),
        operation_id,
        ProducerFailureKind::ExecutionUnavailable,
    );
    assert!(
        terminal
            .effects()
            .iter()
            .all(|effect| !matches!(effect, ProducerEffect::ReleasePayload { .. }))
    );
    assert!(!producer.admission_is_open());
}

fn admit_waiting(producer: &mut ProducerMachine) -> crate::OperationId {
    producer
        .apply(ProducerInput::AdmitWaiting {
            now: Moment::from_tick(0),
            deadline: Deadline::from_tick(10),
            retained_bytes: ByteCount::new(11),
        })
        .unwrap_or_else(|error| panic!("waiting admission failed: {error}"))
        .admitted_operation_id()
        .unwrap_or_else(|| panic!("waiting operation identity"))
}

fn request_flush(producer: &mut ProducerMachine) {
    let flush = producer
        .apply(ProducerInput::FlushRequested)
        .unwrap_or_else(|error| panic!("flush admission failed: {error}"));
    assert!(matches!(
        flush.effects(),
        [ProducerEffect::AcceptFlush { .. }]
    ));
}

fn assert_record_then_flush(
    effects: &[ProducerEffect],
    operation_id: crate::OperationId,
    expected: ProducerFailureKind,
) {
    assert!(matches!(
        effects,
        [
            ProducerEffect::Complete {
                operation_id: actual,
                completion: ProducerCompletion::Failed(failure),
            },
            ProducerEffect::CompleteFlush { .. },
        ] if *actual == operation_id
            && failure.kind() == expected
            && failure.delivery() == DeliveryStatus::NotSent
    ));
}
