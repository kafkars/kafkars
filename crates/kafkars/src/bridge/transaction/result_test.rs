//! Exhaustive stable transaction initialization result translation.

use kafka_client_engine::{
    TransactionControlErrorKind, TransactionInitializationAcceptedFaultKind,
    TransactionInitializationAdmissionErrorKind, TransactionInitializationCaptureError,
    TransactionInitializationDeliveryStatus, TransactionInitializationFailureKind,
    TransactionInitializationObserverError,
};

use super::result::{
    translate_accepted_fault, translate_admission_kind, translate_capture_error,
    translate_control_kind, translate_failure_parts, translate_observer_error,
};
use crate::{DeliveryStatus, ErrorKind, RetryAdvice};

#[test]
fn every_local_rejection_category_preserves_pre_admission_retry_safety() {
    let cases = [
        (
            TransactionInitializationAdmissionErrorKind::InvalidRequest,
            ErrorKind::Configuration,
            RetryAdvice::DoNotRetry,
        ),
        (
            TransactionInitializationAdmissionErrorKind::Contended,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            TransactionInitializationAdmissionErrorKind::Closed,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            TransactionInitializationAdmissionErrorKind::Capacity,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            TransactionInitializationAdmissionErrorKind::RetainedBytes,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            TransactionInitializationAdmissionErrorKind::IdentityExhausted,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
        (
            TransactionInitializationAdmissionErrorKind::HostUnavailable,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
    ];
    for (input, expected, retry) in cases {
        let error = translate_admission_kind(input);
        assert_eq!(error.kind(), expected);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
        assert_eq!(error.retry_advice(), retry);
    }
    assert_eq!(
        translate_capture_error(TransactionInitializationCaptureError::InvalidOperationDeadline)
            .kind(),
        ErrorKind::Timeout
    );
}

#[test]
fn terminal_failures_preserve_certainty_fencing_and_exact_broker_code() {
    let cases = [
        (
            TransactionInitializationFailureKind::DeadlineElapsed,
            ErrorKind::Timeout,
            None,
        ),
        (
            TransactionInitializationFailureKind::DriverRejected,
            ErrorKind::Backpressure,
            None,
        ),
        (
            TransactionInitializationFailureKind::Transport,
            ErrorKind::Transport,
            None,
        ),
        (
            TransactionInitializationFailureKind::Broker {
                code: -73,
                fenced: false,
            },
            ErrorKind::Broker,
            Some(-73),
        ),
        (
            TransactionInitializationFailureKind::Broker {
                code: 47,
                fenced: true,
            },
            ErrorKind::Fenced,
            Some(47),
        ),
        (
            TransactionInitializationFailureKind::Broker {
                code: 90,
                fenced: true,
            },
            ErrorKind::Fenced,
            Some(90),
        ),
        (
            TransactionInitializationFailureKind::Broker {
                code: 91,
                fenced: true,
            },
            ErrorKind::Broker,
            Some(91),
        ),
        (
            TransactionInitializationFailureKind::InvalidResponse,
            ErrorKind::Broker,
            None,
        ),
        (
            TransactionInitializationFailureKind::ExecutionUnavailable,
            ErrorKind::Internal,
            None,
        ),
    ];
    for (input, expected, broker_code) in cases {
        let error =
            translate_failure_parts(input, TransactionInitializationDeliveryStatus::PossiblySent);
        assert_eq!(error.kind(), expected);
        assert_eq!(error.broker_code(), broker_code);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    }
}

#[test]
fn observer_and_accepted_faults_remain_stable_internal_categories() {
    assert_eq!(
        translate_observer_error(TransactionInitializationObserverError::AlreadyObserved).kind(),
        ErrorKind::State
    );
    assert_eq!(
        translate_observer_error(TransactionInitializationObserverError::Stale).kind(),
        ErrorKind::Internal
    );
    for fault in [
        TransactionInitializationAcceptedFaultKind::Wake,
        TransactionInitializationAcceptedFaultKind::HostInvariant,
    ] {
        assert_eq!(translate_accepted_fault(fault).kind(), ErrorKind::Internal);
    }
}

#[test]
fn lifecycle_control_categories_translate_exhaustively() {
    use TransactionControlErrorKind as Kind;
    let cases = [
        (
            Kind::InvalidDeadline,
            ErrorKind::Configuration,
            RetryAdvice::DoNotRetry,
        ),
        (
            Kind::Contended,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (Kind::Closed, ErrorKind::State, RetryAdvice::DoNotRetry),
        (Kind::StaleOwner, ErrorKind::State, RetryAdvice::DoNotRetry),
        (
            Kind::AlreadyActive,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (Kind::NotActive, ErrorKind::State, RetryAdvice::DoNotRetry),
        (
            Kind::StaleTransaction,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            Kind::OutstandingOperations,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            Kind::AbortRequired,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            Kind::EndInProgress,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (Kind::Fenced, ErrorKind::State, RetryAdvice::DoNotRetry),
        (
            Kind::Backpressure,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            Kind::IdentityExhausted,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
        (
            Kind::HostUnavailable,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
    ];
    for (input, expected_kind, expected_retry) in cases {
        let error = translate_control_kind(input);
        assert_eq!(error.kind(), expected_kind);
        assert_eq!(error.retry_advice(), expected_retry);
    }
}
