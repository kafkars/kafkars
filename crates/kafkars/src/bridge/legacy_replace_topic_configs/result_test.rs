//! Legacy replacement translation category and diagnostic tests.

use crate::{DeliveryStatus, ErrorKind};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionErrorKind, DeliveryStatus as EngineDeliveryStatus, FailureKind,
        ObserverError,
    },
    result::{
        translate_accepted_fault, translate_admission_kind, translate_failure_parts,
        translate_observer_error, translate_topic_error_parts,
    },
};

#[test]
fn admission_categories_are_exhaustive_and_definitely_unsent() {
    for (kind, expected) in [
        (AdmissionErrorKind::InvalidRequest, ErrorKind::Configuration),
        (
            AdmissionErrorKind::InvalidDeadline,
            ErrorKind::Configuration,
        ),
        (AdmissionErrorKind::Contended, ErrorKind::Backpressure),
        (AdmissionErrorKind::Capacity, ErrorKind::Backpressure),
        (AdmissionErrorKind::RetainedBytes, ErrorKind::Backpressure),
        (AdmissionErrorKind::Closed, ErrorKind::State),
        (AdmissionErrorKind::IdentityExhausted, ErrorKind::Internal),
        (AdmissionErrorKind::HostUnavailable, ErrorKind::Internal),
    ] {
        let error = translate_admission_kind(kind);
        assert_eq!(error.kind(), expected);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    }
}

#[test]
fn mechanism_failures_preserve_category_and_exact_delivery() {
    for (kind, expected) in [
        (FailureKind::DeadlineElapsed, ErrorKind::Timeout),
        (FailureKind::DriverRejected, ErrorKind::Backpressure),
        (FailureKind::Transport, ErrorKind::Transport),
        (FailureKind::ResponseTooLarge, ErrorKind::Backpressure),
        (FailureKind::Compatibility, ErrorKind::Compatibility),
        (FailureKind::InvalidResponse, ErrorKind::Broker),
    ] {
        for (delivery, expected_delivery) in [
            (EngineDeliveryStatus::NotSent, DeliveryStatus::NotSent),
            (
                EngineDeliveryStatus::PossiblySent,
                DeliveryStatus::PossiblySent,
            ),
        ] {
            let error = translate_failure_parts(kind, delivery);
            assert_eq!(error.kind(), expected);
            assert_eq!(error.delivery_status(), Some(expected_delivery));
        }
    }
}

#[test]
fn topic_error_keeps_exact_signed_code_diagnostic_and_truncation() {
    let error = translate_topic_error_parts(-811, Some("bounded"), true);

    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.broker_code(), Some(-811));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert!(error.diagnostic_truncated());
    assert!(error.to_string().contains("bounded"));
}

#[test]
fn accepted_and_observer_failures_keep_stable_categories() {
    for fault in [AcceptedFaultKind::Wake, AcceptedFaultKind::HostInvariant] {
        assert_eq!(translate_accepted_fault(fault).kind(), ErrorKind::Internal);
    }
    assert_eq!(
        translate_observer_error(ObserverError::AlreadyObserved).kind(),
        ErrorKind::State
    );
    assert_eq!(
        translate_observer_error(ObserverError::Stale).kind(),
        ErrorKind::Internal
    );
}
