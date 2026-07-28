//! Private hosted group close observer contract.

use std::future::Future;

use kafka_client_engine::GroupConsumerCloseErrorKind;

use super::group_consumer_close::{GroupConsumerClose, translate_close_kind};
use crate::{ErrorKind, KafkaError};

#[test]
fn bridge_close_is_one_send_runtime_neutral_observer() {
    fn require<T: Future<Output = Result<(), KafkaError>> + Send>() {}
    fn contract(close: GroupConsumerClose) {
        let _: Option<KafkaError> = close.advisory_error();
        let _: Result<(), KafkaError> = close.wait();
    }

    require::<GroupConsumerClose>();
    let _ = contract as fn(GroupConsumerClose);
}

#[test]
fn every_leave_group_terminal_has_one_stable_facade_category() {
    for (engine, facade) in [
        (
            GroupConsumerCloseErrorKind::DeadlineElapsed,
            ErrorKind::Timeout,
        ),
        (
            GroupConsumerCloseErrorKind::DriverRejected,
            ErrorKind::Backpressure,
        ),
        (GroupConsumerCloseErrorKind::Transport, ErrorKind::Transport),
        (
            GroupConsumerCloseErrorKind::Authentication,
            ErrorKind::Access,
        ),
        (
            GroupConsumerCloseErrorKind::BrokerRejected,
            ErrorKind::Broker,
        ),
        (
            GroupConsumerCloseErrorKind::Compatibility,
            ErrorKind::Compatibility,
        ),
        (
            GroupConsumerCloseErrorKind::InvalidResponse,
            ErrorKind::Internal,
        ),
        (
            GroupConsumerCloseErrorKind::ResponseTooLarge,
            ErrorKind::Internal,
        ),
        (
            GroupConsumerCloseErrorKind::HostUnavailable,
            ErrorKind::Internal,
        ),
        (
            GroupConsumerCloseErrorKind::InternalInvariant,
            ErrorKind::Internal,
        ),
    ] {
        assert_eq!(translate_close_kind(engine, Some(-731)).kind(), facade);
    }
    assert_eq!(
        translate_close_kind(GroupConsumerCloseErrorKind::BrokerRejected, Some(-731)).broker_code(),
        Some(-731)
    );
}
