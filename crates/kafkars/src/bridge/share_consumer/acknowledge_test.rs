//! Private share acknowledgement admission and observer translation contract.

use kafka_client_engine::share::ShareAcknowledgementAdmissionErrorKind as EngineAdmissionErrorKind;

use crate::{DeliveryStatus, ErrorKind, RetryAdvice};

use super::acknowledge::translate_admission_kind;

#[test]
fn acknowledgement_admission_categories_preserve_exact_retry_authority() {
    let categories = [
        (
            EngineAdmissionErrorKind::InvalidDeadline,
            ErrorKind::Configuration,
            RetryAdvice::DoNotRetry,
        ),
        (
            EngineAdmissionErrorKind::ForeignRegistry,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
        (
            EngineAdmissionErrorKind::Closed,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            EngineAdmissionErrorKind::Contended,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            EngineAdmissionErrorKind::Unavailable,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            EngineAdmissionErrorKind::Backpressure,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            EngineAdmissionErrorKind::DeadlineElapsed,
            ErrorKind::Timeout,
            RetryAdvice::DoNotRetry,
        ),
        (
            EngineAdmissionErrorKind::StaleAcknowledgement,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            EngineAdmissionErrorKind::InvalidRequest,
            ErrorKind::Configuration,
            RetryAdvice::DoNotRetry,
        ),
        (
            EngineAdmissionErrorKind::Internal,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
    ];
    for (engine, kind, retry) in categories {
        let error = translate_admission_kind(engine);
        assert_eq!(error.kind(), kind);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
        assert_eq!(error.retry_advice(), retry);
    }
}
