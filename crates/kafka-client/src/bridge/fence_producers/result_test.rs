//! Producer-fencing translation categories, identities, and delivery.

use crate::{DeliveryStatus, ErrorKind};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionErrorKind, DeliveryStatus as EngineDeliveryStatus, FailureKind,
        ObserverError,
    },
    result::{
        translate_accepted_fault, translate_admission_kind, translate_batch_parts,
        translate_broker_error_code, translate_failure_parts, translate_identity_parts,
        translate_observer_error,
    },
};

#[test]
fn throttle_and_caller_order_cross_the_bridge_exactly() {
    let result = translate_batch_parts(
        u32::MAX,
        vec![
            (
                "first".to_owned(),
                Ok(translate_identity_parts(i64::MAX, i16::MAX)),
            ),
            ("second".to_owned(), Err(translate_broker_error_code(-733))),
        ],
    );

    assert_eq!(
        result.throttle_time(),
        std::time::Duration::from_millis(u64::from(u32::MAX))
    );
    let entries = result.producers().entries();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, "first");
    assert_eq!(
        entries[0]
            .1
            .as_ref()
            .unwrap_or_else(|error| panic!("identity expected: {error}"))
            .producer_id(),
        i64::MAX
    );
    assert_eq!(entries[1].0, "second");
    assert_eq!(
        entries[1]
            .1
            .as_ref()
            .expect_err("broker rejection expected")
            .broker_code(),
        Some(-733)
    );
}

#[test]
fn signed_identity_crosses_the_bridge_exactly() {
    let identity = translate_identity_parts(i64::MIN + 17, i16::MIN + 9);
    assert_eq!(identity.producer_id(), i64::MIN + 17);
    assert_eq!(identity.producer_epoch(), i16::MIN + 9);
}

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
fn signed_broker_response_code_is_exact_and_intrinsically_possibly_sent() {
    let error = translate_broker_error_code(-733);
    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.broker_code(), Some(-733));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert!(!error.diagnostic_truncated());
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
