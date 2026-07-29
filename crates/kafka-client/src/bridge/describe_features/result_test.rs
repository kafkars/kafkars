//! Kafka feature discovery category, delivery, and stable result tests.

use crate::{
    DeliveryStatus, ErrorKind,
    admin::{FinalizedFeature, SupportedFeature},
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionErrorKind, DeliveryStatus as EngineDeliveryStatus, FailureKind,
        ObserverError,
    },
    result::{
        translate_accepted_fault, translate_admission_kind, translate_broker_error_parts,
        translate_description_parts, translate_failure_parts, translate_observer_error,
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
fn result_translation_keeps_canonical_ranges_epoch_and_migration_fact() {
    let result = translate_description_parts(
        23,
        vec![SupportedFeature::new(
            String::from("metadata.version"),
            7,
            12,
        )],
        false,
        Some(41),
        vec![FinalizedFeature::new(
            String::from("metadata.version"),
            11,
            11,
        )],
        false,
    );

    assert_eq!(result.throttle_time(), std::time::Duration::from_millis(23));
    assert_eq!(result.supported_features()[0].name(), "metadata.version");
    assert_eq!(result.supported_features()[0].min_version_level(), 7);
    assert_eq!(result.supported_features()[0].max_version_level(), 12);
    assert!(!result.supported_features_complete());
    assert_eq!(result.finalized_features()[0].min_version_level(), 11);
    assert_eq!(result.finalized_features_epoch(), Some(41));
    assert!(!result.zk_migration_ready());
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
fn broker_rejection_keeps_exact_signed_code_and_delivery() {
    let error = translate_broker_error_parts(37, -731);

    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.broker_code(), Some(-731));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
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
