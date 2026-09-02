//! Group seek conversion, observation shape, and exhaustive error mapping.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use kafka_client_engine::{
    GroupConsumerSeekAdmissionErrorKind as Admission, GroupConsumerSeekErrorKind as Terminal,
};

use super::group_consumer_seek::{
    GroupConsumerSeek, engine_partition, translate_admission_kind, translate_terminal_kind,
};
use crate::{
    ErrorKind, KafkaError, RetryAdvice,
    consumer::{StartPosition, TopicPartition},
};

#[test]
fn bridge_seek_is_send_and_runtime_neutral() {
    fn require<T: Future<Output = Result<(), KafkaError>> + Send>() {}
    fn blocking(operation: GroupConsumerSeek<'_>) -> Result<(), KafkaError> {
        operation.wait()
    }

    require::<GroupConsumerSeek<'static>>();
    let _ = blocking;
}

#[test]
fn rejected_seek_is_immediately_ready_without_engine_observation() {
    let mut seek = GroupConsumerSeek::rejected(KafkaError::new(ErrorKind::State, "rejected seek"));
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(
        Pin::new(&mut seek).poll(&mut context),
        Poll::Ready(Err(error)) if error.kind() == ErrorKind::State
    ));
}

#[test]
fn target_conversion_rejects_direct_assignment_policy_and_invalid_scalars() {
    let configured = TopicPartition::new("orders", 0).start_at(StartPosition::Beginning);
    assert_eq!(
        engine_partition(configured)
            .err()
            .unwrap_or_else(|| panic!("configured target must reject"))
            .kind(),
        ErrorKind::Configuration
    );

    for invalid in [
        TopicPartition::new("", 0),
        TopicPartition::new("orders", -1),
        TopicPartition::new("x".repeat(250), 0),
    ] {
        assert_eq!(
            engine_partition(invalid)
                .err()
                .unwrap_or_else(|| panic!("invalid target must reject"))
                .kind(),
            ErrorKind::Configuration
        );
    }
}

#[test]
fn every_admission_kind_has_one_stable_facade_category() {
    for (kind, expected) in [
        (Admission::Contended, ErrorKind::Backpressure),
        (Admission::Pending, ErrorKind::Backpressure),
        (Admission::ResourceExhausted, ErrorKind::Backpressure),
        (Admission::Closed, ErrorKind::State),
        (Admission::GroupUnavailable, ErrorKind::State),
        (Admission::NoActiveAssignment, ErrorKind::State),
        (Admission::UnknownPartition, ErrorKind::State),
        (Admission::NegativeOffset, ErrorKind::Configuration),
        (Admission::HostUnavailable, ErrorKind::Internal),
        (Admission::InternalInvariant, ErrorKind::Internal),
    ] {
        assert_eq!(translate_admission_kind(kind).kind(), expected);
    }
}

#[test]
fn immediate_seek_contention_is_safe_to_retry() {
    for kind in [
        Admission::Contended,
        Admission::Pending,
        Admission::ResourceExhausted,
    ] {
        assert_eq!(
            translate_admission_kind(kind).retry_advice(),
            RetryAdvice::RetrySafe
        );
    }
}

#[test]
fn every_terminal_kind_and_signed_code_have_one_stable_facade_category() {
    for (kind, expected) in [
        (Terminal::DeadlineElapsed, ErrorKind::Timeout),
        (Terminal::DriverRejected, ErrorKind::Backpressure),
        (Terminal::Transport, ErrorKind::Transport),
        (Terminal::BrokerRejected, ErrorKind::Broker),
        (Terminal::Compatibility, ErrorKind::Compatibility),
        (Terminal::AssignmentLost, ErrorKind::State),
        (Terminal::InvalidResponse, ErrorKind::Internal),
        (Terminal::ResponseTooLarge, ErrorKind::Internal),
        (Terminal::HostUnavailable, ErrorKind::Internal),
        (Terminal::InternalInvariant, ErrorKind::Internal),
    ] {
        assert_eq!(translate_terminal_kind(kind, None).kind(), expected);
    }
    assert_eq!(
        translate_terminal_kind(Terminal::BrokerRejected, Some(47)).broker_code(),
        Some(47)
    );
}
