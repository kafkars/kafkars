//! Exhaustive translation of engine receive failures.

use kafka_client_engine::{AssignedConsumerRecvError, AssignedConsumerRecvErrorKind};

use crate::{ErrorKind, KafkaError};

pub(super) fn translate_assigned_consumer_recv(error: AssignedConsumerRecvError) -> KafkaError {
    translate_assigned_consumer_recv_kind(error.kind())
}

pub(super) fn translate_assigned_consumer_recv_kind(
    recv: AssignedConsumerRecvErrorKind,
) -> KafkaError {
    let (kind, message) = match recv {
        AssignedConsumerRecvErrorKind::HostUnavailable => (
            ErrorKind::Internal,
            "assigned-consumer receive owner is unavailable",
        ),
        AssignedConsumerRecvErrorKind::InternalInvariant => (
            ErrorKind::Internal,
            "assigned-consumer receive ownership is inconsistent",
        ),
    };
    KafkaError::new(kind, message)
}
