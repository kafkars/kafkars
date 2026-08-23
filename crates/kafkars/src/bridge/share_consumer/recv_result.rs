//! Exhaustive translation of hosted share receive failures.

use kafka_client_engine::share::{ShareConsumerRecvError, ShareConsumerRecvErrorKind};

use crate::{ErrorKind, KafkaError};

pub(super) fn translate_share_consumer_recv(error: ShareConsumerRecvError) -> KafkaError {
    translate_share_consumer_recv_kind(error.kind())
}

pub(super) fn translate_share_consumer_recv_kind(kind: ShareConsumerRecvErrorKind) -> KafkaError {
    match kind {
        ShareConsumerRecvErrorKind::HostUnavailable => {
            KafkaError::new(ErrorKind::Internal, "share receive host is unavailable")
        }
        ShareConsumerRecvErrorKind::InternalInvariant => KafkaError::new(
            ErrorKind::Internal,
            "share receive ownership is inconsistent",
        ),
    }
}

pub(super) fn internal_recv_error(message: &'static str) -> KafkaError {
    KafkaError::new(ErrorKind::Internal, message)
}
