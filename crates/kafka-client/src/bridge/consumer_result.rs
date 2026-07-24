//! Exhaustive translation of assigned-consumer engine outcomes.

use kafka_client_engine::{
    AssignedConsumerAcceptedFaultKind, AssignedConsumerClaimError,
    AssignedConsumerCloseObserverError, AssignedConsumerTryCloseError,
    AssignedConsumerTryCloseErrorKind,
};

use crate::{ErrorKind, KafkaError};

pub(crate) fn translate_assigned_consumer_claim(error: AssignedConsumerClaimError) -> KafkaError {
    match error {
        AssignedConsumerClaimError::AlreadyClaimed => KafkaError::new(
            ErrorKind::State,
            "this client's assigned consumer was already built",
        ),
        AssignedConsumerClaimError::Poisoned => KafkaError::new(
            ErrorKind::Internal,
            "assigned-consumer claim ownership became unavailable",
        ),
    }
}

pub(crate) fn translate_assigned_close_admission(
    error: AssignedConsumerTryCloseError,
) -> KafkaError {
    translate_assigned_close_admission_kind(error.kind())
}

pub(crate) fn translate_assigned_close_admission_kind(
    kind: AssignedConsumerTryCloseErrorKind,
) -> KafkaError {
    match kind {
        AssignedConsumerTryCloseErrorKind::Contended => KafkaError::new(
            ErrorKind::Backpressure,
            "assigned-consumer close admission is contended",
        ),
        AssignedConsumerTryCloseErrorKind::Closed => {
            KafkaError::new(ErrorKind::State, "assigned consumer is already closed")
        }
        AssignedConsumerTryCloseErrorKind::CompletionCapacity => KafkaError::new(
            ErrorKind::Backpressure,
            "assigned-consumer close completion capacity is occupied",
        ),
        AssignedConsumerTryCloseErrorKind::Pending => KafkaError::new(
            ErrorKind::Backpressure,
            "assigned-consumer work is pending interpretation",
        ),
        AssignedConsumerTryCloseErrorKind::HostUnavailable => KafkaError::new(
            ErrorKind::Internal,
            "assigned-consumer host execution is unavailable",
        ),
        AssignedConsumerTryCloseErrorKind::InternalInvariant => KafkaError::new(
            ErrorKind::Internal,
            "assigned-consumer close ownership is inconsistent",
        ),
    }
}

pub(crate) fn translate_assigned_close_fault(
    fault: AssignedConsumerAcceptedFaultKind,
) -> KafkaError {
    match fault {
        AssignedConsumerAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "assigned-consumer close was accepted but its immediate wake failed",
        ),
    }
}

pub(crate) fn translate_assigned_close_observation(
    error: AssignedConsumerCloseObserverError,
) -> KafkaError {
    match error {
        AssignedConsumerCloseObserverError::ExecutionUnavailable => KafkaError::new(
            ErrorKind::Internal,
            "assigned-consumer close execution became unavailable",
        ),
        AssignedConsumerCloseObserverError::AlreadyObserved => KafkaError::new(
            ErrorKind::State,
            "assigned-consumer close was already observed",
        ),
        AssignedConsumerCloseObserverError::Stale => KafkaError::new(
            ErrorKind::Internal,
            "assigned-consumer close observer lost terminal ownership",
        ),
    }
}
