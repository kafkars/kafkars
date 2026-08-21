//! Exhaustive translation of engine producer cancellation values.

use kafka_client_engine::{
    ProducerCancelError as EngineCancelError, ProducerCancelErrorKind as EngineCancelErrorKind,
    ProducerCancelFault as EngineCancelFault, ProducerCancelFaultKind as EngineCancelFaultKind,
    ProducerCancellationOutcome as EngineCancellationOutcome,
};

use crate::{CancellationOutcome, ErrorKind, KafkaError};

pub(crate) fn translate_cancellation_error(error: &EngineCancelError) -> KafkaError {
    KafkaError::new(cancellation_error_kind(error.kind()), error.to_string())
}

pub(crate) fn translate_cancellation_fault(fault: &EngineCancelFault) -> KafkaError {
    KafkaError::new(cancellation_fault_kind(fault.kind()), fault.to_string())
}

pub(crate) const fn translate_cancellation_outcome(
    outcome: EngineCancellationOutcome,
) -> CancellationOutcome {
    match outcome {
        EngineCancellationOutcome::CancelledNotSent => CancellationOutcome::CancelledNotSent,
        EngineCancellationOutcome::TooLate => CancellationOutcome::TooLate,
        EngineCancellationOutcome::AlreadyTerminal => CancellationOutcome::AlreadyTerminal,
    }
}

pub(super) const fn cancellation_error_kind(kind: EngineCancelErrorKind) -> ErrorKind {
    match kind {
        EngineCancelErrorKind::Contended => ErrorKind::Backpressure,
        EngineCancelErrorKind::HostUnavailable
        | EngineCancelErrorKind::ExecutionGenerationExhausted
        | EngineCancelErrorKind::InternalInvariant => ErrorKind::Internal,
    }
}

pub(super) const fn cancellation_fault_kind(kind: EngineCancelFaultKind) -> ErrorKind {
    match kind {
        EngineCancelFaultKind::Wake => ErrorKind::Internal,
    }
}
