//! Exhaustive stable translation of transaction initialization outcomes.

use kafka_client_engine::{
    TransactionControlErrorKind, TransactionInitializationAcceptedFaultKind,
    TransactionInitializationAdmissionError, TransactionInitializationAdmissionErrorKind,
    TransactionInitializationCaptureError, TransactionInitializationDeliveryStatus,
    TransactionInitializationFailure, TransactionInitializationFailureKind,
    TransactionInitializationObserverError, TransactionInitializationOutcome,
};

use crate::{DeliveryStatus, ErrorKind, KafkaError};

use super::{TransactionalProducerEngine, operation::TransactionInitializationResult};

mod end;
#[cfg(test)]
mod end_test;

#[cfg(test)]
pub(super) use end::translate_end_failure_parts;
pub(super) use end::translate_end_observation;

pub(super) fn translate_control_kind(kind: TransactionControlErrorKind) -> KafkaError {
    let public = match kind {
        TransactionControlErrorKind::InvalidDeadline => ErrorKind::Configuration,
        TransactionControlErrorKind::Contended | TransactionControlErrorKind::Backpressure => {
            ErrorKind::Backpressure
        }
        // A local lifecycle fence has no signed broker code to prove which
        // broker fact caused it. Keep that state distinct from the public
        // broker-fencing category.
        TransactionControlErrorKind::Closed
        | TransactionControlErrorKind::StaleOwner
        | TransactionControlErrorKind::AlreadyActive
        | TransactionControlErrorKind::NotActive
        | TransactionControlErrorKind::StaleTransaction
        | TransactionControlErrorKind::OutstandingOperations
        | TransactionControlErrorKind::AbortRequired
        | TransactionControlErrorKind::EndInProgress
        | TransactionControlErrorKind::Fenced => ErrorKind::State,
        TransactionControlErrorKind::IdentityExhausted
        | TransactionControlErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    let error = KafkaError::new(public, format!("transaction control rejected: {kind:?}"));
    match kind {
        TransactionControlErrorKind::Contended | TransactionControlErrorKind::Backpressure => {
            error.with_safe_retry()
        }
        TransactionControlErrorKind::InvalidDeadline
        | TransactionControlErrorKind::Closed
        | TransactionControlErrorKind::StaleOwner
        | TransactionControlErrorKind::AlreadyActive
        | TransactionControlErrorKind::NotActive
        | TransactionControlErrorKind::StaleTransaction
        | TransactionControlErrorKind::OutstandingOperations
        | TransactionControlErrorKind::AbortRequired
        | TransactionControlErrorKind::EndInProgress
        | TransactionControlErrorKind::Fenced
        | TransactionControlErrorKind::IdentityExhausted
        | TransactionControlErrorKind::HostUnavailable => error,
    }
}

pub(super) fn translate_capture_error(error: TransactionInitializationCaptureError) -> KafkaError {
    match error {
        TransactionInitializationCaptureError::InvalidOperationDeadline => KafkaError::new(
            ErrorKind::Timeout,
            "transaction initialization deadline cannot be represented",
        )
        .with_delivery_status(DeliveryStatus::NotSent),
    }
}

pub(super) fn translate_admission_error(
    error: &TransactionInitializationAdmissionError,
) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(
    kind: TransactionInitializationAdmissionErrorKind,
) -> KafkaError {
    let public = match kind {
        TransactionInitializationAdmissionErrorKind::InvalidRequest => ErrorKind::Configuration,
        TransactionInitializationAdmissionErrorKind::Contended
        | TransactionInitializationAdmissionErrorKind::Capacity
        | TransactionInitializationAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        TransactionInitializationAdmissionErrorKind::Closed => ErrorKind::State,
        TransactionInitializationAdmissionErrorKind::IdentityExhausted
        | TransactionInitializationAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    let error = KafkaError::new(
        public,
        format!("transaction initialization admission failed: {kind:?}"),
    )
    .with_delivery_status(DeliveryStatus::NotSent);
    match kind {
        TransactionInitializationAdmissionErrorKind::Contended
        | TransactionInitializationAdmissionErrorKind::Capacity
        | TransactionInitializationAdmissionErrorKind::RetainedBytes => error.with_safe_retry(),
        TransactionInitializationAdmissionErrorKind::InvalidRequest
        | TransactionInitializationAdmissionErrorKind::Closed
        | TransactionInitializationAdmissionErrorKind::IdentityExhausted
        | TransactionInitializationAdmissionErrorKind::HostUnavailable => error,
    }
}

pub(super) fn translate_accepted_fault(
    fault: TransactionInitializationAcceptedFaultKind,
) -> KafkaError {
    let message = match fault {
        TransactionInitializationAcceptedFaultKind::Wake => {
            "transaction initialization was accepted but its host wake failed"
        }
        TransactionInitializationAcceptedFaultKind::HostInvariant => {
            "transaction initialization was accepted but its host reported an invariant failure"
        }
    };
    KafkaError::new(ErrorKind::Internal, message)
}

pub(super) fn translate_observation(
    result: Result<TransactionInitializationOutcome, TransactionInitializationObserverError>,
) -> TransactionInitializationResult {
    match result {
        Ok(TransactionInitializationOutcome::Initialized(owner)) => {
            Ok(TransactionalProducerEngine::from_engine(owner))
        }
        Ok(TransactionInitializationOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_failure(failure: TransactionInitializationFailure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(
    kind: TransactionInitializationFailureKind,
    delivery: TransactionInitializationDeliveryStatus,
) -> KafkaError {
    let (public, broker_code) = match kind {
        TransactionInitializationFailureKind::DeadlineElapsed => (ErrorKind::Timeout, None),
        TransactionInitializationFailureKind::DriverRejected => (ErrorKind::Backpressure, None),
        TransactionInitializationFailureKind::Transport => (ErrorKind::Transport, None),
        TransactionInitializationFailureKind::Broker { code, fenced } => (
            if fenced && matches!(code, 47 | 90) {
                ErrorKind::Fenced
            } else {
                ErrorKind::Broker
            },
            Some(code),
        ),
        TransactionInitializationFailureKind::InvalidResponse => (ErrorKind::Broker, None),
        TransactionInitializationFailureKind::ExecutionUnavailable => (ErrorKind::Internal, None),
    };
    KafkaError::new(
        public,
        format!("transaction initialization failed: {kind:?}"),
    )
    .with_broker_code(broker_code)
    .with_delivery_status(translate_delivery(delivery))
}

const fn translate_delivery(delivery: TransactionInitializationDeliveryStatus) -> DeliveryStatus {
    match delivery {
        TransactionInitializationDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        TransactionInitializationDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

pub(super) fn translate_observer_error(
    error: TransactionInitializationObserverError,
) -> KafkaError {
    let public = match error {
        TransactionInitializationObserverError::AlreadyObserved => ErrorKind::State,
        TransactionInitializationObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}

pub(super) fn already_observed() -> KafkaError {
    KafkaError::new(
        ErrorKind::State,
        "transaction initialization was already observed",
    )
}
