//! Exhaustive stable transaction initialization result translation.

use kafka_client_engine::{
    TransactionInitializationAcceptedFaultKind, TransactionInitializationAdmissionErrorKind,
    TransactionInitializationCaptureError, TransactionInitializationDeliveryStatus,
    TransactionInitializationFailureKind, TransactionInitializationObserverError,
};

use super::result::{
    translate_accepted_fault, translate_admission_kind, translate_capture_error,
    translate_failure_parts, translate_observer_error,
};
use crate::{DeliveryStatus, ErrorKind};

#[test]
fn every_local_rejection_category_maps_without_hidden_retry_policy() {
    let cases = [
        (
            TransactionInitializationAdmissionErrorKind::InvalidRequest,
            ErrorKind::Configuration,
        ),
        (
            TransactionInitializationAdmissionErrorKind::Contended,
            ErrorKind::Backpressure,
        ),
        (
            TransactionInitializationAdmissionErrorKind::Closed,
            ErrorKind::State,
        ),
        (
            TransactionInitializationAdmissionErrorKind::Capacity,
            ErrorKind::Backpressure,
        ),
        (
            TransactionInitializationAdmissionErrorKind::RetainedBytes,
            ErrorKind::Backpressure,
        ),
        (
            TransactionInitializationAdmissionErrorKind::IdentityExhausted,
            ErrorKind::Internal,
        ),
        (
            TransactionInitializationAdmissionErrorKind::HostUnavailable,
            ErrorKind::Internal,
        ),
    ];
    for (input, expected) in cases {
        let error = translate_admission_kind(input);
        assert_eq!(error.kind(), expected);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
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
                code: 90,
                fenced: true,
            },
            ErrorKind::Fenced,
            Some(90),
        ),
        (
            TransactionInitializationFailureKind::InvalidResponse,
            ErrorKind::Broker,
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
