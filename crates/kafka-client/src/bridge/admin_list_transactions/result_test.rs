//! ListTransactions error-category and exact-delivery translation tests.

use crate::{DeliveryStatus, ErrorKind};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionErrorKind, DeliveryStatus as EngineDeliveryStatus, FailureKind,
        ObserverError,
    },
    result::{
        translate_accepted_fault, translate_admission_kind, translate_discovery_parts,
        translate_failure_parts, translate_listed_parts, translate_observer_error,
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
fn failures_preserve_category_and_exact_delivery() {
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

#[test]
fn listed_parts_restore_global_order_and_preserve_signed_scalars() {
    let result = translate_listed_parts(
        31,
        vec![
            "Future".to_owned(),
            "Another".to_owned(),
            "Future".to_owned(),
        ],
        vec![
            ("zeta".to_owned(), i64::MIN, "FutureState".to_owned()),
            ("alpha".to_owned(), -1, "Ongoing".to_owned()),
        ],
        vec![(9, -17), (2, -32_000)],
    );
    assert_eq!(result.throttle_time(), std::time::Duration::from_millis(31));
    assert_eq!(result.unknown_state_filters(), ["Another", "Future"]);
    assert_eq!(result.transactions()[0].transactional_id(), "alpha");
    assert_eq!(result.transactions()[0].producer_id(), -1);
    assert_eq!(result.transactions()[1].producer_id(), i64::MIN);
    assert_eq!(result.broker_errors()[0].broker_id(), 2);
    assert_eq!(result.broker_errors()[0].code(), -32_000);
}

#[test]
fn discovery_rejection_preserves_exact_code_delivery_and_diagnostic() {
    let error = translate_discovery_parts(-731, Some("controller detail"), true);
    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.broker_code(), Some(-731));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert!(error.to_string().contains("controller detail"));
    assert!(error.diagnostic_truncated());
}
