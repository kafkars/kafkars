//! DescribeProducers category, delivery, diagnostic, and scalar tests.

use crate::{DeliveryStatus, ErrorKind};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionErrorKind, DeliveryStatus as EngineDeliveryStatus, FailureKind,
        ObserverError,
    },
    result::{
        producer_state_from_parts, translate_accepted_fault, translate_admission_kind,
        translate_broker_error_parts, translate_failure_parts, translate_observer_error,
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
fn broker_rejection_keeps_signed_code_nullable_diagnostic_and_truncation() {
    let with_message = translate_broker_error_parts(-731, Some("partition detail"), true);
    let without_message = translate_broker_error_parts(-732, None, false);

    assert_eq!(with_message.kind(), ErrorKind::Broker);
    assert_eq!(with_message.broker_code(), Some(-731));
    assert_eq!(
        with_message.delivery_status(),
        Some(DeliveryStatus::PossiblySent)
    );
    assert!(with_message.to_string().contains("partition detail"));
    assert!(with_message.diagnostic_truncated());
    assert_eq!(without_message.broker_code(), Some(-732));
    assert!(!without_message.diagnostic_truncated());
}

#[test]
fn producer_translation_preserves_exact_sentinels_and_transaction_absence() {
    let state = producer_state_from_parts(71, 4, -1, -1, 9, None);

    assert_eq!(state.producer_id(), 71);
    assert_eq!(state.producer_epoch(), 4);
    assert_eq!(state.last_sequence(), -1);
    assert_eq!(state.last_timestamp(), -1);
    assert_eq!(state.coordinator_epoch(), 9);
    assert_eq!(state.current_transaction_start_offset(), None);
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
