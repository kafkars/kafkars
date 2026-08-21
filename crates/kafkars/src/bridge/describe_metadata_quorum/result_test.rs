//! Metadata-quorum error-category, scope, and delivery-certainty tests.

use crate::{DeliveryStatus, ErrorKind};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionErrorKind, DeliveryStatus as EngineDeliveryStatus, FailureKind,
        ObserverError,
    },
    result::{
        translate_accepted_fault, translate_admission_kind, translate_broker_error_parts,
        translate_failure_parts, translate_observer_error,
    },
};

#[test]
fn admission_categories_are_exhaustive_and_definitely_unsent() {
    for (kind, expected) in [
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
fn top_level_and_partition_rejections_keep_exact_codes_and_distinct_scope() {
    let top = translate_broker_error_parts(-731, Some("top"), true, "top-level");
    let partition =
        translate_broker_error_parts(-732, Some("partition"), false, "metadata-partition");

    assert_eq!(top.kind(), ErrorKind::Broker);
    assert_eq!(top.broker_code(), Some(-731));
    assert_eq!(top.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert!(top.to_string().contains("top-level"));
    assert!(top.diagnostic_truncated());
    assert_eq!(partition.broker_code(), Some(-732));
    assert!(partition.to_string().contains("metadata-partition"));
    assert!(!partition.diagnostic_truncated());
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
