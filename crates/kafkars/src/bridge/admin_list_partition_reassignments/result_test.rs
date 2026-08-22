//! Stable translation category smoke tests.

use kafka_client_engine::{
    ListPartitionReassignmentsAcceptedFaultKind, ListPartitionReassignmentsAdmissionErrorKind,
};

use crate::{DeliveryStatus, ErrorKind, RetryAdvice};

use super::result::{translate_accepted_fault, translate_admission_kind, translate_broker_parts};

#[test]
fn only_transient_pre_admission_categories_are_safe_to_retry() {
    for (kind, expected, retry) in [
        (
            ListPartitionReassignmentsAdmissionErrorKind::InvalidRequest,
            ErrorKind::Configuration,
            RetryAdvice::DoNotRetry,
        ),
        (
            ListPartitionReassignmentsAdmissionErrorKind::InvalidDeadline,
            ErrorKind::Configuration,
            RetryAdvice::DoNotRetry,
        ),
        (
            ListPartitionReassignmentsAdmissionErrorKind::Contended,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            ListPartitionReassignmentsAdmissionErrorKind::Closed,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            ListPartitionReassignmentsAdmissionErrorKind::Capacity,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            ListPartitionReassignmentsAdmissionErrorKind::RetainedBytes,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            ListPartitionReassignmentsAdmissionErrorKind::IdentityExhausted,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
        (
            ListPartitionReassignmentsAdmissionErrorKind::HostUnavailable,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
    ] {
        let error = translate_admission_kind(kind);
        assert_eq!(error.kind(), expected);
        assert_eq!(error.retry_advice(), retry);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    }
}

#[test]
fn accepted_faults_remain_internal_diagnostics() {
    for fault in [
        ListPartitionReassignmentsAcceptedFaultKind::Wake,
        ListPartitionReassignmentsAcceptedFaultKind::HostInvariant,
    ] {
        assert_eq!(translate_accepted_fault(fault).kind(), ErrorKind::Internal);
    }
}

#[test]
fn broker_diagnostic_preserves_signed_code_truncation_and_delivery() {
    let error = translate_broker_parts(
        -31_999,
        Some("controller diagnostic".to_owned()),
        true,
        DeliveryStatus::PossiblySent,
    );

    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.broker_code(), Some(-31_999));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert!(error.diagnostic_truncated());
    assert!(error.to_string().contains("controller diagnostic"));
    assert!(error.to_string().contains("truncated"));
}
