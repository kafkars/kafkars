//! Exhaustive producer cancellation translation scenarios.

use kafka_client_engine::{
    ProducerCancelErrorKind as EngineCancelErrorKind,
    ProducerCancelFaultKind as EngineCancelFaultKind,
    ProducerCancellationOutcome as EngineCancellationOutcome,
};

use super::cancellation::{
    cancellation_error_kind, cancellation_fault_kind, translate_cancellation_outcome,
};
use crate::{CancellationOutcome, ErrorKind};

#[test]
fn every_engine_cancellation_outcome_has_one_public_outcome() {
    let cases = [
        (
            EngineCancellationOutcome::CancelledNotSent,
            CancellationOutcome::CancelledNotSent,
        ),
        (
            EngineCancellationOutcome::TooLate,
            CancellationOutcome::TooLate,
        ),
        (
            EngineCancellationOutcome::AlreadyTerminal,
            CancellationOutcome::AlreadyTerminal,
        ),
    ];

    for (engine, facade) in cases {
        assert_eq!(translate_cancellation_outcome(engine), facade);
    }
}

#[test]
fn every_cancellation_attempt_failure_has_one_stable_category() {
    let cases = [
        (EngineCancelErrorKind::Contended, ErrorKind::Backpressure),
        (EngineCancelErrorKind::HostUnavailable, ErrorKind::Internal),
        (
            EngineCancelErrorKind::ExecutionGenerationExhausted,
            ErrorKind::Internal,
        ),
        (
            EngineCancelErrorKind::InternalInvariant,
            ErrorKind::Internal,
        ),
    ];

    for (engine, facade) in cases {
        assert_eq!(cancellation_error_kind(engine), facade);
    }
}

#[test]
fn advisory_wake_fault_is_diagnostic_only() {
    assert_eq!(
        cancellation_fault_kind(EngineCancelFaultKind::Wake),
        ErrorKind::Internal
    );
}
