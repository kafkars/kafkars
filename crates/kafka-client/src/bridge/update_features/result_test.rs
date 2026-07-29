//! Finalized-feature outcome, error, and delivery translation tests.

use crate::{DeliveryStatus, ErrorKind};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionErrorKind, DeliveryStatus as EngineDeliveryStatus, FailureKind,
        ObserverError,
    },
    result::{
        translate_accepted_fault, translate_admission_kind, translate_batch_parts,
        translate_broker_parts, translate_failure_parts, translate_feature_parts,
        translate_observer_error,
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
fn mechanism_failures_preserve_category_and_delivery() {
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
            let error = translate_failure_parts(&kind, delivery);
            assert_eq!(error.kind(), expected);
            assert_eq!(error.delivery_status(), Some(expected_delivery));
        }
    }
}

#[test]
fn old_partial_results_and_synthesized_successes_stay_distinct_and_ordered() {
    let first = translate_feature_parts(String::from("metadata.version"), None);
    let second = translate_feature_parts(
        String::from("transaction.version"),
        Some((-731, Some("cannot downgrade"), true)),
    );
    let third = translate_feature_parts(String::from("group.version"), None);

    let result = translate_batch_parts(19, vec![first, second, third]);
    assert_eq!(result.throttle_time(), std::time::Duration::from_millis(19));
    assert_eq!(result.features().entries()[0].0, "metadata.version");
    assert!(result.features().entries()[0].1.is_ok());
    assert_eq!(result.features().entries()[1].0, "transaction.version");
    let error = result.features().entries()[1]
        .1
        .as_ref()
        .err()
        .unwrap_or_else(|| panic!("old broker per-feature failure expected"));
    assert_eq!(error.broker_code(), Some(-731));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert!(error.diagnostic_truncated());
    assert_eq!(result.features().entries()[2].0, "group.version");
    assert!(result.features().entries()[2].1.is_ok());
}

#[test]
fn broker_diagnostics_keep_exact_code_scope_truncation_and_delivery() {
    let error = translate_broker_parts(
        "operation",
        -1234,
        Some(""),
        true,
        DeliveryStatus::PossiblySent,
    );

    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.broker_code(), Some(-1234));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert!(error.diagnostic_truncated());
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
