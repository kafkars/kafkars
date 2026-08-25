//! Exhaustive stable `CreatePartitions` translation scenarios.

use kafka_client_engine::{
    CreatePartitionsAcceptedFaultKind, CreatePartitionsAdmissionErrorKind,
    CreatePartitionsDeliveryStatus, CreatePartitionsFailureKind, CreatePartitionsObserverError,
};

use super::admin_partitions_result::{
    translate_accepted_fault, translate_admission_kind, translate_failure_parts,
    translate_observer_error, translate_topic_error_parts,
};
use crate::{DeliveryStatus, ErrorKind, RetryAdvice};

#[test]
fn every_admission_category_maps_without_hidden_retry_policy() {
    let cases = [
        (
            CreatePartitionsAdmissionErrorKind::InvalidRequest,
            ErrorKind::Configuration,
        ),
        (
            CreatePartitionsAdmissionErrorKind::InvalidDeadline,
            ErrorKind::Configuration,
        ),
        (
            CreatePartitionsAdmissionErrorKind::Contended,
            ErrorKind::Backpressure,
        ),
        (CreatePartitionsAdmissionErrorKind::Closed, ErrorKind::State),
        (
            CreatePartitionsAdmissionErrorKind::Capacity,
            ErrorKind::Backpressure,
        ),
        (
            CreatePartitionsAdmissionErrorKind::RetainedBytes,
            ErrorKind::Backpressure,
        ),
        (
            CreatePartitionsAdmissionErrorKind::IdentityExhausted,
            ErrorKind::Internal,
        ),
        (
            CreatePartitionsAdmissionErrorKind::HostUnavailable,
            ErrorKind::Internal,
        ),
    ];
    for (engine, public) in cases {
        let error = translate_admission_kind(engine);
        assert_eq!(error.kind(), public);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
        let expected = match engine {
            CreatePartitionsAdmissionErrorKind::Contended
            | CreatePartitionsAdmissionErrorKind::Capacity
            | CreatePartitionsAdmissionErrorKind::RetainedBytes => RetryAdvice::RetrySafe,
            CreatePartitionsAdmissionErrorKind::InvalidRequest
            | CreatePartitionsAdmissionErrorKind::InvalidDeadline
            | CreatePartitionsAdmissionErrorKind::Closed
            | CreatePartitionsAdmissionErrorKind::IdentityExhausted
            | CreatePartitionsAdmissionErrorKind::HostUnavailable => RetryAdvice::DoNotRetry,
        };
        assert_eq!(error.retry_advice(), expected);
    }
}

#[test]
fn failures_codes_and_diagnostics_are_lossless() {
    let failure = translate_failure_parts(
        CreatePartitionsFailureKind::Transport,
        CreatePartitionsDeliveryStatus::PossiblySent,
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
fn driver_deadline_is_public_timeout_with_authoritative_certainty() {
    let failure = translate_failure_parts(
        CreatePartitionsFailureKind::DeadlineElapsed,
        CreatePartitionsDeliveryStatus::PossiblySent,
    );
    assert_eq!(failure.kind(), ErrorKind::Timeout);
    assert_eq!(
        failure.delivery_status(),
        Some(DeliveryStatus::PossiblySent)
    );
}

#[test]
fn observer_and_accepted_fault_categories_remain_distinct() {
    assert_eq!(
        translate_observer_error(CreatePartitionsObserverError::AlreadyObserved).kind(),
        ErrorKind::State
    );
    assert_eq!(
        translate_observer_error(CreatePartitionsObserverError::Stale).kind(),
        ErrorKind::Internal
    );
    for fault in [
        CreatePartitionsAcceptedFaultKind::Wake,
        CreatePartitionsAcceptedFaultKind::HostInvariant,
    ] {
        assert_eq!(translate_accepted_fault(fault).kind(), ErrorKind::Internal);
    }
}
