//! SCRAM description value, error, and delivery translation tests.

use crate::{DeliveryStatus, ErrorKind, admin::ScramMechanism};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionErrorKind, DeliveryStatus as EngineDeliveryStatus, FailureKind,
        ObserverError,
    },
    result::{
        translate_accepted_fault, translate_admission_kind, translate_failure_parts,
        translate_info_parts, translate_observer_error, translate_user_error_parts,
    },
};

#[test]
fn credential_metadata_preserves_future_mechanism_codes() {
    let info = translate_info_parts(-41, 8192);

    assert_eq!(info.mechanism(), ScramMechanism::from_code(-41));
    assert_eq!(info.iterations(), 8192);
}

#[test]
fn admission_categories_remain_exhaustive_and_definitely_unsent() {
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
fn accepted_faults_remain_advisory_internal_diagnostics() {
    for fault in [AcceptedFaultKind::Wake, AcceptedFaultKind::HostInvariant] {
        assert_eq!(translate_accepted_fault(fault).kind(), ErrorKind::Internal);
    }
}

#[test]
fn mechanism_failures_preserve_category_and_delivery() {
    for (kind, expected) in [
        (FailureKind::DeadlineElapsed, ErrorKind::Timeout),
        (FailureKind::DriverRejected, ErrorKind::Backpressure),
        (FailureKind::Transport, ErrorKind::Transport),
        (FailureKind::ResponseTooLarge, ErrorKind::Backpressure),
        (FailureKind::Compatibility, ErrorKind::Compatibility),
        (FailureKind::InvalidResponse, ErrorKind::Broker),
    ] {
        let error = translate_failure_parts(kind, EngineDeliveryStatus::PossiblySent);
        assert_eq!(error.kind(), expected);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    }
}

#[test]
fn user_error_code_message_truncation_and_delivery_are_exact() {
    let error = translate_user_error_parts(-731, Some("denied"), true);

    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.broker_code(), Some(-731));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert!(error.diagnostic_truncated());
}

#[test]
fn observer_failures_keep_state_and_internal_categories() {
    assert_eq!(
        translate_observer_error(ObserverError::AlreadyObserved).kind(),
        ErrorKind::State
    );
    assert_eq!(
        translate_observer_error(ObserverError::Stale).kind(),
        ErrorKind::Internal
    );
}
