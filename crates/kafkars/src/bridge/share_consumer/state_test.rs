//! Exhaustive facade mapping for share membership observation.

use kafka_client_engine::share::{ShareConsumerStartupFailureKind, ShareConsumerStateErrorKind};

use super::state::{translate_startup_failure, translate_state_error};
use crate::{ErrorKind, RetryAdvice};

#[test]
fn state_observation_distinguishes_pending_capacity_from_lifecycle_failure() {
    for (source, kind, retry) in [
        (
            ShareConsumerStateErrorKind::Contended,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            ShareConsumerStateErrorKind::Allocation,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            ShareConsumerStateErrorKind::Unavailable,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            ShareConsumerStateErrorKind::Internal,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
    ] {
        let error = translate_state_error(source);
        assert_eq!(error.kind(), kind);
        assert_eq!(error.retry_advice(), retry);
    }
}

#[test]
fn every_retained_startup_terminal_is_fatal_and_preserves_broker_code() {
    for (source, kind, code) in [
        (
            ShareConsumerStartupFailureKind::CoordinatorUnavailable,
            ErrorKind::Routing,
            None,
        ),
        (
            ShareConsumerStartupFailureKind::Compatibility,
            ErrorKind::Compatibility,
            None,
        ),
        (
            ShareConsumerStartupFailureKind::Execution,
            ErrorKind::Internal,
            None,
        ),
        (
            ShareConsumerStartupFailureKind::Broker(16),
            ErrorKind::Broker,
            Some(16),
        ),
        (
            ShareConsumerStartupFailureKind::InvalidResponse,
            ErrorKind::Internal,
            None,
        ),
        (
            ShareConsumerStartupFailureKind::DeadlineElapsed,
            ErrorKind::Timeout,
            None,
        ),
    ] {
        let error = translate_startup_failure(source);
        assert_eq!(error.kind(), kind);
        assert_eq!(error.broker_code(), code);
        assert!(error.is_fatal());
    }
}
