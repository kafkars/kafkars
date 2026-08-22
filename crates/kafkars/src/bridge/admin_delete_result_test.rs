//! Exhaustive stable `DeleteTopics` translation scenarios.

use kafka_client_engine::{
    DeleteTopicsAcceptedFaultKind, DeleteTopicsAdmissionErrorKind, DeleteTopicsDeliveryStatus,
    DeleteTopicsFailureKind, DeleteTopicsObserverError,
};

use super::admin_delete_result::{
    translate_accepted_fault, translate_admission_kind, translate_failure_parts,
    translate_observer_error, translate_topic_error_parts,
};
use crate::{DeliveryStatus, ErrorKind, RetryAdvice};

#[test]
fn every_admission_category_preserves_pre_admission_retry_safety() {
    let cases = [
        (
            DeleteTopicsAdmissionErrorKind::InvalidRequest,
            ErrorKind::Configuration,
            RetryAdvice::DoNotRetry,
        ),
        (
            DeleteTopicsAdmissionErrorKind::InvalidDeadline,
            ErrorKind::Configuration,
            RetryAdvice::DoNotRetry,
        ),
        (
            DeleteTopicsAdmissionErrorKind::Contended,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            DeleteTopicsAdmissionErrorKind::Closed,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            DeleteTopicsAdmissionErrorKind::Capacity,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            DeleteTopicsAdmissionErrorKind::RetainedBytes,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            DeleteTopicsAdmissionErrorKind::IdentityExhausted,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
        (
            DeleteTopicsAdmissionErrorKind::HostUnavailable,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
    ];
    for (engine, public, retry) in cases {
        let error = translate_admission_kind(engine);
        assert_eq!(error.kind(), public);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
        assert_eq!(error.retry_advice(), retry);
    }
}

#[test]
fn failures_codes_and_diagnostics_are_lossless() {
    let failure = translate_failure_parts(
        DeleteTopicsFailureKind::Transport,
        DeleteTopicsDeliveryStatus::PossiblySent,
    );
    assert_eq!(failure.kind(), ErrorKind::Transport);
    assert_eq!(
        failure.delivery_status(),
        Some(DeliveryStatus::PossiblySent)
    );
    let broker = translate_topic_error_parts(-32_000, Some("bounded"), true);
    assert_eq!(broker.broker_code(), Some(-32_000));
    assert!(broker.diagnostic_truncated());
}

#[test]
fn observer_and_accepted_fault_categories_remain_distinct() {
    assert_eq!(
        translate_observer_error(DeleteTopicsObserverError::AlreadyObserved).kind(),
        ErrorKind::State
    );
    assert_eq!(
        translate_observer_error(DeleteTopicsObserverError::Stale).kind(),
        ErrorKind::Internal
    );
    for fault in [
        DeleteTopicsAcceptedFaultKind::Wake,
        DeleteTopicsAcceptedFaultKind::HostInvariant,
    ] {
        assert_eq!(translate_accepted_fault(fault).kind(), ErrorKind::Internal);
    }
}
