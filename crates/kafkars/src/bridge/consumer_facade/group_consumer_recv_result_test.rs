//! Hosted group receive failure translation contract.

use kafka_client_engine::{
    GroupConsumerFetchFailureKind, GroupConsumerPositionFailureKind, GroupConsumerRecvErrorKind,
};

use crate::ErrorKind;

use super::group_consumer_recv_result::translate_group_consumer_recv_kind;

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
fn every_fetch_failure_maps_to_its_exact_public_category() {
    for (failure, expected) in [
        (
            GroupConsumerFetchFailureKind::DeadlineElapsed,
            ErrorKind::Timeout,
        ),
        (
            GroupConsumerFetchFailureKind::DriverRejected,
            ErrorKind::Backpressure,
        ),
        (
            GroupConsumerFetchFailureKind::Transport,
            ErrorKind::Transport,
        ),
        (
            GroupConsumerFetchFailureKind::Compatibility,
            ErrorKind::Compatibility,
        ),
        (
            GroupConsumerFetchFailureKind::InvalidResponse,
            ErrorKind::Internal,
        ),
        (
            GroupConsumerFetchFailureKind::ResponseTooLarge,
            ErrorKind::Backpressure,
        ),
        (
            GroupConsumerFetchFailureKind::ThrottleDeadlineOverflow,
            ErrorKind::Timeout,
        ),
    ] {
        let error = translate_group_consumer_recv_kind(GroupConsumerRecvErrorKind::Fetch(failure));
        assert_eq!(error.kind(), expected);
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
