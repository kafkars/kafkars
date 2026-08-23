//! Exhaustive facade mapping for share close admission.

use kafka_client_engine::share::ShareConsumerCloseAdmissionErrorKind;

use super::close::translate_close_admission;
use crate::{ErrorKind, RetryAdvice};

#[test]
fn only_pre_admission_contention_exposes_safe_close_retry() {
    for (source, kind, retry) in [
        (
            ShareConsumerCloseAdmissionErrorKind::Closed,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            ShareConsumerCloseAdmissionErrorKind::Contended,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            ShareConsumerCloseAdmissionErrorKind::Unavailable,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            ShareConsumerCloseAdmissionErrorKind::Internal,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
    ] {
        let error = translate_close_admission(source);
        assert_eq!(error.kind(), kind);
        assert_eq!(error.retry_advice(), retry);
    }
}
