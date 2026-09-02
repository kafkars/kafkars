//! Stable facade mapping for classic-group event and revocation observation.

use kafka_client_engine::{
    GroupConsumerRevocationAcknowledgeErrorKind, GroupConsumerTryTakeEventErrorKind,
};

use super::group_consumer_rebalance_event::{
    translate_group_consumer_event_observation_kind,
    translate_group_consumer_revocation_acknowledgment_kind,
};
use crate::{ErrorKind, RetryAdvice};

#[test]
fn event_observation_categories_translate_exhaustively() {
    for (engine, facade) in [
        (
            GroupConsumerTryTakeEventErrorKind::Contended,
            ErrorKind::Backpressure,
        ),
        (
            GroupConsumerTryTakeEventErrorKind::HostUnavailable,
            ErrorKind::Internal,
        ),
        (
            GroupConsumerTryTakeEventErrorKind::InternalInvariant,
            ErrorKind::Internal,
        ),
    ] {
        assert_eq!(
            translate_group_consumer_event_observation_kind(engine).kind(),
            facade
        );
    }
}

#[test]
fn event_and_revocation_contention_are_safe_to_retry() {
    assert_eq!(
        translate_group_consumer_event_observation_kind(
            GroupConsumerTryTakeEventErrorKind::Contended,
        )
        .retry_advice(),
        RetryAdvice::RetrySafe
    );
    assert_eq!(
        translate_group_consumer_revocation_acknowledgment_kind(
            GroupConsumerRevocationAcknowledgeErrorKind::Contended,
        )
        .retry_advice(),
        RetryAdvice::RetrySafe
    );
}

#[test]
fn revocation_acknowledgment_categories_translate_exhaustively() {
    use GroupConsumerRevocationAcknowledgeErrorKind as Kind;
    for (engine, facade) in [
        (Kind::Closed, ErrorKind::State),
        (Kind::Contended, ErrorKind::Backpressure),
        (Kind::HostUnavailable, ErrorKind::Internal),
        (Kind::GroupUnavailable, ErrorKind::State),
        (Kind::StaleAssignmentEpoch, ErrorKind::State),
        (Kind::DeadlineElapsed, ErrorKind::State),
        (Kind::Clock, ErrorKind::Internal),
        (Kind::InternalInvariant, ErrorKind::Internal),
    ] {
        assert_eq!(
            translate_group_consumer_revocation_acknowledgment_kind(engine).kind(),
            facade
        );
    }
}
