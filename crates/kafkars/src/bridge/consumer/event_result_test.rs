//! Stable facade error mapping for immediate event observation.

use kafka_client_engine::AssignedConsumerTryTakeEventErrorKind;

use super::event_result::translate_assigned_event_observation_kind;
use crate::{ErrorKind, RetryAdvice};

#[test]
fn event_observation_categories_translate_exhaustively() {
    for (engine, facade) in [
        (
            AssignedConsumerTryTakeEventErrorKind::Contended,
            ErrorKind::Backpressure,
        ),
        (
            AssignedConsumerTryTakeEventErrorKind::HostUnavailable,
            ErrorKind::Internal,
        ),
        (
            AssignedConsumerTryTakeEventErrorKind::InternalInvariant,
            ErrorKind::Internal,
        ),
    ] {
        assert_eq!(
            translate_assigned_event_observation_kind(engine).kind(),
            facade
        );
    }
}

#[test]
fn contention_is_retry_safe_without_inventing_network_delivery() {
    let error =
        translate_assigned_event_observation_kind(AssignedConsumerTryTakeEventErrorKind::Contended);
    assert_eq!(error.retry_advice(), RetryAdvice::RetrySafe);
    assert_eq!(error.delivery_status(), None);
}

#[test]
fn owner_and_invariant_failures_do_not_advise_retry() {
    for kind in [
        AssignedConsumerTryTakeEventErrorKind::HostUnavailable,
        AssignedConsumerTryTakeEventErrorKind::InternalInvariant,
    ] {
        let error = translate_assigned_event_observation_kind(kind);
        assert_eq!(error.retry_advice(), RetryAdvice::DoNotRetry);
        assert_eq!(error.delivery_status(), None);
    }
}
