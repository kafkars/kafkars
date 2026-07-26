//! Exhaustive translation of assigned event wait failures.

use kafka_client_engine::{AssignedConsumerNextEventError, AssignedConsumerNextEventErrorKind};

use crate::{ErrorKind, KafkaError};

pub(super) fn translate_assigned_consumer_next_event(
    error: AssignedConsumerNextEventError,
) -> KafkaError {
    translate_assigned_consumer_next_event_kind(error.kind())
}

pub(super) fn translate_assigned_consumer_next_event_kind(
    kind: AssignedConsumerNextEventErrorKind,
) -> KafkaError {
    let message = match kind {
        AssignedConsumerNextEventErrorKind::HostUnavailable => {
            "assigned-consumer event owner is unavailable"
        }
        AssignedConsumerNextEventErrorKind::InternalInvariant => {
            "assigned-consumer event observation is inconsistent"
        }
    };
    KafkaError::new(ErrorKind::Internal, message)
}
