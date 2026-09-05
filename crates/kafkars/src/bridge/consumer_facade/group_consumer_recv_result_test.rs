//! Hosted group receive failure translation contract.

use kafka_client_engine::{
    GroupConsumerFetchFailureKind, GroupConsumerPositionFailureKind, GroupConsumerRecvErrorKind,
    GroupConsumerTryTakeBatchErrorKind,
};

use crate::{ErrorKind, RetryAdvice};

use super::group_consumer::translate_group_consumer_batch_take_kind;
use super::group_consumer_batch::GroupConsumerHeader;
use super::group_consumer_recv_result::translate_group_consumer_recv_kind;

#[test]
fn immediate_batch_contention_is_safe_to_retry() {
    for kind in [
        GroupConsumerTryTakeBatchErrorKind::Contended,
        GroupConsumerTryTakeBatchErrorKind::Pending,
    ] {
        assert_eq!(
            translate_group_consumer_batch_take_kind(kind).retry_advice(),
            RetryAdvice::RetrySafe
        );
    }
}

#[test]
fn group_header_parts_retain_the_record_lifetime_after_translation() {
    type HeaderParts<'record> = (&'record [u8], Option<&'record [u8]>);
    type HeaderContract = for<'record> fn(GroupConsumerHeader<'record>) -> HeaderParts<'record>;

    #[expect(
        clippy::needless_pass_by_value,
        reason = "consuming the iterator item proves returned references retain the record lifetime"
    )]
    fn consume(header: GroupConsumerHeader<'_>) -> HeaderParts<'_> {
        let key = header.key();
        let value = header.value();
        (key, value)
    }

    let _: HeaderContract = consume;
}

#[test]
fn host_failures_remain_internal() {
    for kind in [
        GroupConsumerRecvErrorKind::HostUnavailable,
        GroupConsumerRecvErrorKind::InternalInvariant,
    ] {
        assert_eq!(
            translate_group_consumer_recv_kind(kind).kind(),
            ErrorKind::Internal
        );
    }
}

#[test]
fn every_fetch_failure_maps_to_its_exact_public_category_and_retry_advice() {
    for (failure, expected_kind, expected_retry) in [
        (
            GroupConsumerFetchFailureKind::DeadlineElapsed,
            ErrorKind::Timeout,
            RetryAdvice::RetrySafe,
        ),
        (
            GroupConsumerFetchFailureKind::DriverRejected,
            ErrorKind::Backpressure,
            RetryAdvice::DoNotRetry,
        ),
        (
            GroupConsumerFetchFailureKind::Transport,
            ErrorKind::Transport,
            RetryAdvice::DoNotRetry,
        ),
        (
            GroupConsumerFetchFailureKind::Compatibility,
            ErrorKind::Compatibility,
            RetryAdvice::DoNotRetry,
        ),
        (
            GroupConsumerFetchFailureKind::InvalidResponse,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
        (
            GroupConsumerFetchFailureKind::ResponseTooLarge,
            ErrorKind::Backpressure,
            RetryAdvice::DoNotRetry,
        ),
        (
            GroupConsumerFetchFailureKind::ThrottleDeadlineOverflow,
            ErrorKind::Timeout,
            RetryAdvice::DoNotRetry,
        ),
    ] {
        let error = translate_group_consumer_recv_kind(GroupConsumerRecvErrorKind::Fetch(failure));
        assert_eq!(error.kind(), expected_kind);
        assert_eq!(error.retry_advice(), expected_retry);
        assert_eq!(error.broker_code(), None);
    }
}

#[test]
fn fetch_broker_failure_preserves_the_exact_signed_code() {
    let error = translate_group_consumer_recv_kind(GroupConsumerRecvErrorKind::Fetch(
        GroupConsumerFetchFailureKind::Broker(-731),
    ));

    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.broker_code(), Some(-731));
    assert_eq!(error.retry_advice(), RetryAdvice::DoNotRetry);
}

#[test]
fn every_position_failure_maps_to_its_exact_public_category() {
    for (failure, expected) in [
        (
            GroupConsumerPositionFailureKind::MissingOffset,
            ErrorKind::State,
        ),
        (
            GroupConsumerPositionFailureKind::DeadlineElapsed,
            ErrorKind::Timeout,
        ),
        (
            GroupConsumerPositionFailureKind::DriverRejected,
            ErrorKind::Backpressure,
        ),
        (
            GroupConsumerPositionFailureKind::Transport,
            ErrorKind::Transport,
        ),
        (
            GroupConsumerPositionFailureKind::Compatibility,
            ErrorKind::Compatibility,
        ),
        (
            GroupConsumerPositionFailureKind::InvalidResponse,
            ErrorKind::Internal,
        ),
        (
            GroupConsumerPositionFailureKind::ResponseTooLarge,
            ErrorKind::Backpressure,
        ),
    ] {
        let error =
            translate_group_consumer_recv_kind(GroupConsumerRecvErrorKind::Position(failure));
        assert_eq!(error.kind(), expected);
        assert_eq!(error.broker_code(), None);
    }
}

#[test]
fn position_broker_failure_preserves_the_exact_signed_code() {
    let error = translate_group_consumer_recv_kind(GroupConsumerRecvErrorKind::Position(
        GroupConsumerPositionFailureKind::Broker(-19_731),
    ));

    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.broker_code(), Some(-19_731));
}
