//! Exhaustive translation of hosted group receive failures.

use kafka_client_engine::{
    GroupConsumerPositionFailureKind, GroupConsumerRecvError, GroupConsumerRecvErrorKind,
};

use crate::{ErrorKind, KafkaError};

pub(super) fn translate_group_consumer_recv(error: GroupConsumerRecvError) -> KafkaError {
    translate_group_consumer_recv_kind(error.kind())
}

pub(super) fn translate_group_consumer_recv_kind(kind: GroupConsumerRecvErrorKind) -> KafkaError {
    match kind {
        GroupConsumerRecvErrorKind::Position(failure) => {
            translate_group_consumer_position_failure(failure)
        }
        GroupConsumerRecvErrorKind::HostUnavailable => {
            KafkaError::new(ErrorKind::Internal, "group receive host is unavailable")
        }
        GroupConsumerRecvErrorKind::InternalInvariant => KafkaError::new(
            ErrorKind::Internal,
            "group receive ownership is inconsistent",
        ),
    }
}

pub(super) fn translate_group_consumer_position_failure(
    failure: GroupConsumerPositionFailureKind,
) -> KafkaError {
    match failure {
        GroupConsumerPositionFailureKind::MissingOffset => {
            KafkaError::new(ErrorKind::State, "group assignment has no committed offset")
        }
        GroupConsumerPositionFailureKind::DeadlineElapsed => {
            KafkaError::new(ErrorKind::Timeout, "group position deadline elapsed")
        }
        GroupConsumerPositionFailureKind::DriverRejected => KafkaError::new(
            ErrorKind::Backpressure,
            "group position driver admission was rejected",
        ),
        GroupConsumerPositionFailureKind::Transport => {
            KafkaError::new(ErrorKind::Transport, "group position transport failed")
        }
        GroupConsumerPositionFailureKind::Broker(code) => KafkaError::new(
            ErrorKind::Broker,
            "Kafka rejected group position resolution",
        )
        .with_broker_code(Some(code)),
        GroupConsumerPositionFailureKind::Compatibility => KafkaError::new(
            ErrorKind::Compatibility,
            "no compatible group position protocol is available",
        ),
        GroupConsumerPositionFailureKind::InvalidResponse => KafkaError::new(
            ErrorKind::Internal,
            "group position returned an invalid response",
        ),
        GroupConsumerPositionFailureKind::ResponseTooLarge => KafkaError::new(
            ErrorKind::Backpressure,
            "group position response exceeded configured bounds",
        ),
    }
}

pub(super) fn internal_recv_error(message: &'static str) -> KafkaError {
    KafkaError::new(ErrorKind::Internal, message)
}
