//! Exhaustive translation of direct-assignment inputs and admission outcomes.

use kafka_client_engine::{
    AssignedConsumerAcceptedFaultKind, AssignedConsumerAssignmentInputError,
    AssignedConsumerAssignmentInputErrorKind, AssignedConsumerTryChangeAssignmentError,
    AssignedConsumerTryChangeAssignmentErrorKind, AssignedConsumerTryReplaceAssignmentError,
    AssignedConsumerTryReplaceAssignmentErrorKind,
};

use crate::{ErrorKind, KafkaError};

pub(crate) fn translate_assigned_assignment_input(
    error: AssignedConsumerAssignmentInputError,
) -> KafkaError {
    let message = match error.kind() {
        AssignedConsumerAssignmentInputErrorKind::EmptyTopic => {
            "assigned topic name must not be empty"
        }
        AssignedConsumerAssignmentInputErrorKind::TopicTooLong => {
            "assigned topic name exceeds Kafka's length limit"
        }
        AssignedConsumerAssignmentInputErrorKind::NegativePartition => {
            "assigned partition must be nonnegative"
        }
        AssignedConsumerAssignmentInputErrorKind::NegativeOffset => {
            "assigned start offset must be nonnegative"
        }
    };
    KafkaError::new(ErrorKind::Configuration, message)
}

pub(crate) fn translate_assigned_assignment_admission(
    error: AssignedConsumerTryReplaceAssignmentError,
) -> KafkaError {
    translate_assigned_assignment_admission_kind(error.kind())
}

pub(crate) fn translate_assigned_assignment_admission_kind(
    kind: AssignedConsumerTryReplaceAssignmentErrorKind,
) -> KafkaError {
    let (facade_kind, message) = match kind {
        AssignedConsumerTryReplaceAssignmentErrorKind::Contended => (
            ErrorKind::Backpressure,
            "assigned-consumer replacement is contended",
        ),
        AssignedConsumerTryReplaceAssignmentErrorKind::Closed => {
            (ErrorKind::State, "assigned consumer is closed")
        }
        AssignedConsumerTryReplaceAssignmentErrorKind::Pending => (
            ErrorKind::Backpressure,
            "assigned-consumer work is pending interpretation",
        ),
        AssignedConsumerTryReplaceAssignmentErrorKind::AssignmentCapacity => (
            ErrorKind::Backpressure,
            "assigned partition capacity is exhausted",
        ),
        AssignedConsumerTryReplaceAssignmentErrorKind::TopicCapacity => (
            ErrorKind::Backpressure,
            "assigned topic capacity is exhausted",
        ),
        AssignedConsumerTryReplaceAssignmentErrorKind::RetainedNameCapacity => (
            ErrorKind::Backpressure,
            "assigned topic-name byte capacity is exhausted",
        ),
        AssignedConsumerTryReplaceAssignmentErrorKind::EventCapacity => (
            ErrorKind::Backpressure,
            "assigned-consumer event capacity is exhausted",
        ),
        AssignedConsumerTryReplaceAssignmentErrorKind::EmptyAssignment => (
            ErrorKind::Configuration,
            "direct assignment must contain at least one partition",
        ),
        AssignedConsumerTryReplaceAssignmentErrorKind::DuplicatePartition => (
            ErrorKind::Configuration,
            "direct assignment contains a duplicate topic-partition",
        ),
        AssignedConsumerTryReplaceAssignmentErrorKind::DeadlineOverflow => (
            ErrorKind::Timeout,
            "assignment resolution deadline cannot be represented",
        ),
        AssignedConsumerTryReplaceAssignmentErrorKind::ResourceExhausted => (
            ErrorKind::Internal,
            "assigned-consumer identity or allocation space is exhausted",
        ),
        AssignedConsumerTryReplaceAssignmentErrorKind::HostUnavailable => (
            ErrorKind::Internal,
            "assigned-consumer host execution is unavailable",
        ),
        AssignedConsumerTryReplaceAssignmentErrorKind::InternalInvariant => (
            ErrorKind::Internal,
            "assigned-consumer replacement ownership is inconsistent",
        ),
    };
    let error = KafkaError::new(facade_kind, message);
    match kind {
        AssignedConsumerTryReplaceAssignmentErrorKind::Contended
        | AssignedConsumerTryReplaceAssignmentErrorKind::Pending => error.with_safe_retry(),
        AssignedConsumerTryReplaceAssignmentErrorKind::Closed
        | AssignedConsumerTryReplaceAssignmentErrorKind::AssignmentCapacity
        | AssignedConsumerTryReplaceAssignmentErrorKind::TopicCapacity
        | AssignedConsumerTryReplaceAssignmentErrorKind::RetainedNameCapacity
        | AssignedConsumerTryReplaceAssignmentErrorKind::EventCapacity
        | AssignedConsumerTryReplaceAssignmentErrorKind::EmptyAssignment
        | AssignedConsumerTryReplaceAssignmentErrorKind::DuplicatePartition
        | AssignedConsumerTryReplaceAssignmentErrorKind::DeadlineOverflow
        | AssignedConsumerTryReplaceAssignmentErrorKind::ResourceExhausted
        | AssignedConsumerTryReplaceAssignmentErrorKind::HostUnavailable
        | AssignedConsumerTryReplaceAssignmentErrorKind::InternalInvariant => error,
    }
}

pub(crate) fn translate_assigned_change_admission(
    error: AssignedConsumerTryChangeAssignmentError,
) -> KafkaError {
    translate_assigned_change_admission_kind(error.kind())
}

pub(crate) fn translate_assigned_change_admission_kind(
    kind: AssignedConsumerTryChangeAssignmentErrorKind,
) -> KafkaError {
    let (facade_kind, message) = match kind {
        AssignedConsumerTryChangeAssignmentErrorKind::Contended => (
            ErrorKind::Backpressure,
            "assigned-consumer assignment change is contended",
        ),
        AssignedConsumerTryChangeAssignmentErrorKind::Closed => {
            (ErrorKind::State, "assigned consumer is closed")
        }
        AssignedConsumerTryChangeAssignmentErrorKind::Pending => (
            ErrorKind::Backpressure,
            "assigned-consumer work is pending interpretation",
        ),
        AssignedConsumerTryChangeAssignmentErrorKind::AssignmentCapacity => (
            ErrorKind::Backpressure,
            "assigned partition capacity is exhausted",
        ),
        AssignedConsumerTryChangeAssignmentErrorKind::TopicCapacity => (
            ErrorKind::Backpressure,
            "assigned topic capacity is exhausted",
        ),
        AssignedConsumerTryChangeAssignmentErrorKind::RetainedNameCapacity => (
            ErrorKind::Backpressure,
            "assigned topic-name byte capacity is exhausted",
        ),
        AssignedConsumerTryChangeAssignmentErrorKind::EventCapacity => (
            ErrorKind::Backpressure,
            "assigned-consumer event capacity is exhausted",
        ),
        AssignedConsumerTryChangeAssignmentErrorKind::DuplicatePartition => (
            ErrorKind::Configuration,
            "assignment change contains a duplicate topic-partition",
        ),
        AssignedConsumerTryChangeAssignmentErrorKind::AlreadyAssigned => (
            ErrorKind::State,
            "assignment addition contains an already-assigned topic-partition",
        ),
        AssignedConsumerTryChangeAssignmentErrorKind::NoAssignment => {
            (ErrorKind::State, "assigned consumer has no assignment")
        }
        AssignedConsumerTryChangeAssignmentErrorKind::UnknownPartition => (
            ErrorKind::State,
            "assignment removal contains an unassigned topic-partition",
        ),
        AssignedConsumerTryChangeAssignmentErrorKind::DeadlineOverflow => (
            ErrorKind::Timeout,
            "assignment resolution deadline cannot be represented",
        ),
        AssignedConsumerTryChangeAssignmentErrorKind::ResourceExhausted => (
            ErrorKind::Internal,
            "assigned-consumer identity or allocation space is exhausted",
        ),
        AssignedConsumerTryChangeAssignmentErrorKind::HostUnavailable => (
            ErrorKind::Internal,
            "assigned-consumer host execution is unavailable",
        ),
        AssignedConsumerTryChangeAssignmentErrorKind::InternalInvariant => (
            ErrorKind::Internal,
            "assigned-consumer assignment-change ownership is inconsistent",
        ),
    };
    let error = KafkaError::new(facade_kind, message);
    match kind {
        AssignedConsumerTryChangeAssignmentErrorKind::Contended
        | AssignedConsumerTryChangeAssignmentErrorKind::Pending => error.with_safe_retry(),
        AssignedConsumerTryChangeAssignmentErrorKind::Closed
        | AssignedConsumerTryChangeAssignmentErrorKind::AssignmentCapacity
        | AssignedConsumerTryChangeAssignmentErrorKind::TopicCapacity
        | AssignedConsumerTryChangeAssignmentErrorKind::RetainedNameCapacity
        | AssignedConsumerTryChangeAssignmentErrorKind::EventCapacity
        | AssignedConsumerTryChangeAssignmentErrorKind::DuplicatePartition
        | AssignedConsumerTryChangeAssignmentErrorKind::AlreadyAssigned
        | AssignedConsumerTryChangeAssignmentErrorKind::NoAssignment
        | AssignedConsumerTryChangeAssignmentErrorKind::UnknownPartition
        | AssignedConsumerTryChangeAssignmentErrorKind::DeadlineOverflow
        | AssignedConsumerTryChangeAssignmentErrorKind::ResourceExhausted
        | AssignedConsumerTryChangeAssignmentErrorKind::HostUnavailable
        | AssignedConsumerTryChangeAssignmentErrorKind::InternalInvariant => error,
    }
}

pub(crate) fn translate_assigned_assignment_fault(
    fault: AssignedConsumerAcceptedFaultKind,
) -> KafkaError {
    match fault {
        AssignedConsumerAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "assignment was accepted but its immediate host wake failed",
        ),
    }
}
