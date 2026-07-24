//! Exhaustive facade translation of immediate event-observation failures.

use kafka_client_engine::{
    AssignedConsumerTryTakeEventError, AssignedConsumerTryTakeEventErrorKind,
};

use crate::{ErrorKind, KafkaError};

pub(super) fn translate_assigned_event_observation(
    error: AssignedConsumerTryTakeEventError,
) -> KafkaError {
    translate_assigned_event_observation_kind(error.kind())
}

pub(super) fn translate_assigned_event_observation_kind(
    kind: AssignedConsumerTryTakeEventErrorKind,
) -> KafkaError {
    let (kind, message) = match kind {
        AssignedConsumerTryTakeEventErrorKind::Contended => (
            ErrorKind::Backpressure,
            "assigned-consumer event observation is contended",
        ),
        AssignedConsumerTryTakeEventErrorKind::HostUnavailable => (
            ErrorKind::Internal,
            "assigned-consumer event owner is unavailable",
        ),
        AssignedConsumerTryTakeEventErrorKind::InternalInvariant => (
            ErrorKind::Internal,
            "assigned-consumer event ownership is inconsistent",
        ),
    };
    KafkaError::new(kind, message)
}
