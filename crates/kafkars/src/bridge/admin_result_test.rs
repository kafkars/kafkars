//! Exhaustive stable admin translation scenarios.

use kafka_client_engine::{
    CreateTopicsAcceptedFaultKind, CreateTopicsAdmissionErrorKind, CreateTopicsDeliveryStatus,
    CreateTopicsFailureKind, CreateTopicsObserverError,
};

use super::admin_result::{
    translate_accepted_fault, translate_admission_kind, translate_failure_parts,
    translate_observer_error, translate_topic_error_parts,
};
use crate::{DeliveryStatus, ErrorKind, RetryAdvice};

#[test]
fn every_admission_category_preserves_pre_admission_retry_safety() {
    let cases = [
        (
            CreateTopicsAdmissionErrorKind::InvalidRequest,
            ErrorKind::Configuration,
        ),
        (
            CreateTopicsAdmissionErrorKind::InvalidDeadline,
            ErrorKind::Configuration,
        ),
        (
            CreateTopicsAdmissionErrorKind::Contended,
            ErrorKind::Backpressure,
        ),
        (CreateTopicsAdmissionErrorKind::Closed, ErrorKind::State),
        (
            CreateTopicsAdmissionErrorKind::Capacity,
            ErrorKind::Backpressure,
        ),
        (
            CreateTopicsAdmissionErrorKind::RetainedBytes,
            ErrorKind::Backpressure,
        ),
        (
            CreateTopicsAdmissionErrorKind::IdentityExhausted,
            ErrorKind::Internal,
        ),
        (
            CreateTopicsAdmissionErrorKind::HostUnavailable,
            ErrorKind::Internal,
        ),
    ];
    for (engine, public) in cases {
        let error = translate_admission_kind(engine);
        assert_eq!(error.kind(), public);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
        let expected = match engine {
            CreateTopicsAdmissionErrorKind::Contended
            | CreateTopicsAdmissionErrorKind::Capacity
            | CreateTopicsAdmissionErrorKind::RetainedBytes => RetryAdvice::RetrySafe,
            CreateTopicsAdmissionErrorKind::InvalidRequest
            | CreateTopicsAdmissionErrorKind::InvalidDeadline
            | CreateTopicsAdmissionErrorKind::Closed
            | CreateTopicsAdmissionErrorKind::IdentityExhausted
            | CreateTopicsAdmissionErrorKind::HostUnavailable => RetryAdvice::DoNotRetry,
        };
        assert_eq!(error.retry_advice(), expected);
    }
}

#[test]
fn whole_failure_preserves_authoritative_delivery_certainty() {
    let cases = [
        (
            CreateTopicsFailureKind::DeadlineElapsed,
            CreateTopicsDeliveryStatus::NotSent,
            ErrorKind::Timeout,
            DeliveryStatus::NotSent,
        ),
        (
            CreateTopicsFailureKind::DriverRejected,
            CreateTopicsDeliveryStatus::NotSent,
            ErrorKind::Backpressure,
            DeliveryStatus::NotSent,
        ),
        (
            CreateTopicsFailureKind::Transport,
            CreateTopicsDeliveryStatus::PossiblySent,
            ErrorKind::Transport,
            DeliveryStatus::PossiblySent,
        ),
        (
            CreateTopicsFailureKind::InvalidResponse,
            CreateTopicsDeliveryStatus::PossiblySent,
            ErrorKind::Broker,
            DeliveryStatus::PossiblySent,
        ),
    ];
    for (engine_kind, engine_delivery, public_kind, public_delivery) in cases {
        let error = translate_failure_parts(engine_kind, engine_delivery);
        assert_eq!(error.kind(), public_kind);
        assert_eq!(error.delivery_status(), Some(public_delivery));
    }
}

#[test]
fn unknown_signed_code_and_bounded_diagnostic_are_lossless() {
    let error = translate_topic_error_parts(-32_000, Some("bounded"), true);

    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.broker_code(), Some(-32_000));
    assert!(error.diagnostic_truncated());
}

#[test]
fn observer_and_accepted_fault_categories_remain_distinct() {
    for (observer, public) in [
        (CreateTopicsObserverError::AlreadyObserved, ErrorKind::State),
        (CreateTopicsObserverError::Stale, ErrorKind::Internal),
    ] {
        assert_eq!(translate_observer_error(observer).kind(), public);
    }
    for fault in [
        CreateTopicsAcceptedFaultKind::Wake,
        CreateTopicsAcceptedFaultKind::HostInvariant,
    ] {
        let diagnostic = translate_accepted_fault(fault);
        assert_eq!(diagnostic.kind(), ErrorKind::Internal);
        assert_eq!(diagnostic.delivery_status(), None);
    }
}
