//! Exhaustive stable Admin `ListOffsets` admission translation scenarios.

use kafka_client_engine::AdminListOffsetsAdmissionErrorKind;

use super::result::translate_admission_kind;
use crate::{DeliveryStatus, ErrorKind, RetryAdvice};

#[test]
fn only_pre_admission_resource_pressure_is_retry_safe() {
    let cases = [
        (
            AdminListOffsetsAdmissionErrorKind::InvalidRequest,
            ErrorKind::Configuration,
        ),
        (
            AdminListOffsetsAdmissionErrorKind::InvalidDeadline,
            ErrorKind::Configuration,
        ),
        (
            AdminListOffsetsAdmissionErrorKind::Contended,
            ErrorKind::Backpressure,
        ),
        (AdminListOffsetsAdmissionErrorKind::Closed, ErrorKind::State),
        (
            AdminListOffsetsAdmissionErrorKind::Capacity,
            ErrorKind::Backpressure,
        ),
        (
            AdminListOffsetsAdmissionErrorKind::RetainedBytes,
            ErrorKind::Backpressure,
        ),
        (
            AdminListOffsetsAdmissionErrorKind::IdentityExhausted,
            ErrorKind::Internal,
        ),
        (
            AdminListOffsetsAdmissionErrorKind::HostUnavailable,
            ErrorKind::Internal,
        ),
    ];

    for (engine, public) in cases {
        let error = translate_admission_kind(engine);
        assert_eq!(error.kind(), public);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
        let expected = match engine {
            AdminListOffsetsAdmissionErrorKind::Contended
            | AdminListOffsetsAdmissionErrorKind::Capacity
            | AdminListOffsetsAdmissionErrorKind::RetainedBytes => RetryAdvice::RetrySafe,
            AdminListOffsetsAdmissionErrorKind::InvalidRequest
            | AdminListOffsetsAdmissionErrorKind::InvalidDeadline
            | AdminListOffsetsAdmissionErrorKind::Closed
            | AdminListOffsetsAdmissionErrorKind::IdentityExhausted
            | AdminListOffsetsAdmissionErrorKind::HostUnavailable => RetryAdvice::DoNotRetry,
        };
        assert_eq!(error.retry_advice(), expected);
    }
}
