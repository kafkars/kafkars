//! Public pre-admission retry-safety translation tests.

use kafka_client_engine::DescribeConsumerGroupsAdmissionErrorKind as AdmissionErrorKind;

use super::result::translate_admission_kind;
use crate::{DeliveryStatus, ErrorKind, RetryAdvice};

#[test]
fn every_admission_category_preserves_pre_admission_retry_safety() {
    for (kind, expected_kind, expected_retry) in [
        (
            AdmissionErrorKind::InvalidRequest,
            ErrorKind::Configuration,
            RetryAdvice::DoNotRetry,
        ),
        (
            AdmissionErrorKind::InvalidDeadline,
            ErrorKind::Configuration,
            RetryAdvice::DoNotRetry,
        ),
        (
            AdmissionErrorKind::Contended,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            AdmissionErrorKind::Capacity,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            AdmissionErrorKind::RetainedBytes,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            AdmissionErrorKind::Closed,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            AdmissionErrorKind::IdentityExhausted,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
        (
            AdmissionErrorKind::HostUnavailable,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
    ] {
        let error = translate_admission_kind(kind);
        assert_eq!(error.kind(), expected_kind);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
        assert_eq!(error.retry_advice(), expected_retry);
    }
}
