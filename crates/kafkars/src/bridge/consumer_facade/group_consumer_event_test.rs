//! Scenarios for truthful retry advice on immediate group-state observation.

use kafka_client_engine::GroupConsumerStateErrorKind;

use super::group_consumer_event::translate_group_consumer_state_error_kind;
use crate::{ErrorKind, RetryAdvice};

#[test]
fn only_pre_observation_backpressure_is_safe_to_retry() {
    for kind in [
        GroupConsumerStateErrorKind::Contended,
        GroupConsumerStateErrorKind::Allocation,
    ] {
        let error = translate_group_consumer_state_error_kind(kind);
        assert_eq!(error.kind(), ErrorKind::Backpressure, "{kind:?}");
        assert_eq!(error.retry_advice(), RetryAdvice::RetrySafe, "{kind:?}");
    }

    for kind in [
        GroupConsumerStateErrorKind::HostUnavailable,
        GroupConsumerStateErrorKind::InternalInvariant,
    ] {
        let error = translate_group_consumer_state_error_kind(kind);
        assert_eq!(error.kind(), ErrorKind::Internal, "{kind:?}");
        assert_eq!(error.retry_advice(), RetryAdvice::DoNotRetry, "{kind:?}");
    }
}
