//! Exhaustive stable `DeleteTopics` translation scenarios.

use kafka_client_engine::{
    DeleteTopicsAcceptedFaultKind, DeleteTopicsAdmissionErrorKind, DeleteTopicsDeliveryStatus,
    DeleteTopicsFailureKind, DeleteTopicsObserverError,
};

use super::admin_delete_result::{
    translate_accepted_fault, translate_admission_kind, translate_failure_parts,
    translate_observer_error, translate_topic_error_parts,
};
use crate::{DeliveryStatus, ErrorKind};

#[test]
fn every_admission_category_maps_without_hidden_retry_policy() {
    let cases = [
        (
            DeleteTopicsAdmissionErrorKind::InvalidRequest,
            ErrorKind::Configuration,
        ),
        (
            DeleteTopicsAdmissionErrorKind::InvalidDeadline,
            ErrorKind::Configuration,
        ),
        (
            DeleteTopicsAdmissionErrorKind::Contended,
            ErrorKind::Backpressure,
        ),
        (DeleteTopicsAdmissionErrorKind::Closed, ErrorKind::State),
        (
            DeleteTopicsAdmissionErrorKind::Capacity,
            ErrorKind::Backpressure,
        ),
        (
            DeleteTopicsAdmissionErrorKind::RetainedBytes,
            ErrorKind::Backpressure,
        ),
        (
            DeleteTopicsAdmissionErrorKind::IdentityExhausted,
            ErrorKind::Internal,
        ),
        (
            DeleteTopicsAdmissionErrorKind::HostUnavailable,
            ErrorKind::Internal,
        ),
    ];
    for (engine, public) in cases {
        let error = translate_admission_kind(engine);
        assert_eq!(error.kind(), public);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
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
