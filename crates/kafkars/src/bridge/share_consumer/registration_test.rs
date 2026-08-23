//! Exhaustive facade mapping for share registration admission.

use kafka_client_engine::share::ShareConsumerRegistrationErrorKind;

use super::registration::translate_registration_kind;
use crate::{ErrorKind, RetryAdvice};

#[test]
fn pre_admission_categories_preserve_exact_retry_safety() {
    for (source, kind, retry) in [
        (
            ShareConsumerRegistrationErrorKind::Closed,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            ShareConsumerRegistrationErrorKind::Contended,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            ShareConsumerRegistrationErrorKind::Backpressure,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            ShareConsumerRegistrationErrorKind::InvalidInput,
            ErrorKind::Configuration,
            RetryAdvice::DoNotRetry,
        ),
        (
            ShareConsumerRegistrationErrorKind::Internal,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
    ] {
        let error = translate_registration_kind(source);
        assert_eq!(error.kind(), kind);
        assert_eq!(error.retry_advice(), retry);
    }
}
