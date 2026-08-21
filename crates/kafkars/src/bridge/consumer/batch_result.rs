//! Exhaustive facade translation of immediate batch-observation failures.

use kafka_client_engine::{
    AssignedConsumerTryTakeBatchError, AssignedConsumerTryTakeBatchErrorKind,
};

use crate::{ErrorKind, KafkaError};

pub(crate) fn translate_assigned_batch_observation(
    error: AssignedConsumerTryTakeBatchError,
) -> KafkaError {
    translate_assigned_batch_observation_kind(error.kind())
}

pub(crate) fn translate_assigned_batch_observation_kind(
    kind: AssignedConsumerTryTakeBatchErrorKind,
) -> KafkaError {
    let (kind, message) = match kind {
        AssignedConsumerTryTakeBatchErrorKind::Contended => (
            ErrorKind::Backpressure,
            "assigned-consumer batch observation is contended",
        ),
        AssignedConsumerTryTakeBatchErrorKind::Closed => {
            (ErrorKind::State, "assigned consumer is already closed")
        }
        AssignedConsumerTryTakeBatchErrorKind::Pending => (
            ErrorKind::Backpressure,
            "assigned-consumer work is pending interpretation",
        ),
        AssignedConsumerTryTakeBatchErrorKind::HostUnavailable => (
            ErrorKind::Internal,
            "assigned-consumer host execution is unavailable",
        ),
        AssignedConsumerTryTakeBatchErrorKind::InternalInvariant => (
            ErrorKind::Internal,
            "assigned-consumer delivery ownership is inconsistent",
        ),
    };
    KafkaError::new(kind, message)
}
