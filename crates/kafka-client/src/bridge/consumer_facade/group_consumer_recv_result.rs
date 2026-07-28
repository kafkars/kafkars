//! Exhaustive translation of hosted group receive failures.

use kafka_client_engine::{GroupConsumerRecvError, GroupConsumerRecvErrorKind};

use crate::{ErrorKind, KafkaError};

pub(super) fn translate_group_consumer_recv(error: GroupConsumerRecvError) -> KafkaError {
    let kind = error.kind();
    let message = match kind {
        GroupConsumerRecvErrorKind::HostUnavailable => "group receive host is unavailable",
        GroupConsumerRecvErrorKind::InternalInvariant => "group receive ownership is inconsistent",
    };
    KafkaError::new(translate_group_consumer_recv_kind(kind), message)
}

pub(super) const fn translate_group_consumer_recv_kind(
    kind: GroupConsumerRecvErrorKind,
) -> ErrorKind {
    match kind {
        GroupConsumerRecvErrorKind::HostUnavailable
        | GroupConsumerRecvErrorKind::InternalInvariant => ErrorKind::Internal,
    }
}

pub(super) fn internal_recv_error(message: &'static str) -> KafkaError {
    KafkaError::new(ErrorKind::Internal, message)
}
