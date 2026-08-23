//! Private share acknowledgement terminal translation contract.

use kafka_client_engine::share::ShareAcknowledgementObserverError as EngineObserverError;

use crate::RetryAdvice;

use super::acknowledge_result::translate_observer_error;

#[test]
fn observer_lifecycle_failures_never_invent_retry_ownership() {
    for observer in [
        EngineObserverError::AlreadyObserved,
        EngineObserverError::Stale,
    ] {
        let error = translate_observer_error(observer);
        let (retry, semantic) = error.into_parts();
        assert!(retry.is_none());
        assert_eq!(semantic.retry_advice(), RetryAdvice::DoNotRetry);
        assert_eq!(semantic.delivery_status(), None);
    }
}
