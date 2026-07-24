//! Exhaustive translation of direct-consumer control inputs and outcomes.

use kafka_client_engine::{
    AssignedConsumerControlError, AssignedConsumerControlErrorKind,
    AssignedConsumerPartitionInputError, AssignedConsumerPartitionInputErrorKind,
};

use crate::{ErrorKind, KafkaError};

pub(crate) fn translate_assigned_control_input(
    error: AssignedConsumerPartitionInputError,
) -> KafkaError {
    let message = match error.kind() {
        AssignedConsumerPartitionInputErrorKind::EmptyTopic => {
            "consumer control topic must not be empty"
        }
        AssignedConsumerPartitionInputErrorKind::TopicTooLong => {
            "consumer control topic exceeds Kafka's length limit"
        }
        AssignedConsumerPartitionInputErrorKind::NegativePartition => {
            "consumer control partition must be nonnegative"
        }
    };
    KafkaError::new(ErrorKind::Configuration, message)
}

pub(crate) fn translate_assigned_control_admission(
    error: AssignedConsumerControlError,
) -> KafkaError {
    translate_assigned_control_admission_kind(error.kind())
}

pub(crate) fn translate_assigned_control_admission_kind(
    kind: AssignedConsumerControlErrorKind,
) -> KafkaError {
    let (facade_kind, message) = match kind {
        AssignedConsumerControlErrorKind::Contended => (
            ErrorKind::Backpressure,
            "assigned-consumer control is contended",
        ),
        AssignedConsumerControlErrorKind::Closed => {
            (ErrorKind::State, "assigned consumer is closed")
        }
        AssignedConsumerControlErrorKind::Pending => (
            ErrorKind::Backpressure,
            "assigned-consumer work is pending interpretation",
        ),
        AssignedConsumerControlErrorKind::NoAssignment => {
            (ErrorKind::State, "assigned consumer has no assignment")
        }
        AssignedConsumerControlErrorKind::StaleAssignment => (
            ErrorKind::State,
            "assigned-consumer assignment was superseded",
        ),
        AssignedConsumerControlErrorKind::UnknownPartition => (
            ErrorKind::State,
            "partition is not in the active direct assignment",
        ),
        AssignedConsumerControlErrorKind::NegativeOffset => (
            ErrorKind::Configuration,
            "consumer seek offset must be nonnegative",
        ),
        AssignedConsumerControlErrorKind::DeadlineOverflow => (
            ErrorKind::Timeout,
            "consumer position deadline cannot be represented",
        ),
        AssignedConsumerControlErrorKind::ResourceExhausted => (
            ErrorKind::Internal,
            "assigned-consumer identity or allocation space is exhausted",
        ),
        AssignedConsumerControlErrorKind::HostUnavailable => (
            ErrorKind::Internal,
            "assigned-consumer host execution is unavailable",
        ),
        AssignedConsumerControlErrorKind::InternalInvariant => (
            ErrorKind::Internal,
            "assigned-consumer control ownership is inconsistent",
        ),
    };
    KafkaError::new(facade_kind, message)
}

pub(crate) fn translate_missing_assignment() -> KafkaError {
    translate_assigned_control_admission_kind(AssignedConsumerControlErrorKind::NoAssignment)
}
