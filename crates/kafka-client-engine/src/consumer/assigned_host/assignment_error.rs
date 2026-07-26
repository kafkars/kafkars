//! Exhaustive translation from private assignment owners to stable engine errors.

use kafka_client_core::AssignedConsumerMachineError;

use crate::{
    clock::ClockError,
    completion::CompletionRegistryError,
    consumer::{
        assigned_event::AssignedConsumerEventStoreError,
        assigned_owner_model::AssignedConsumerOwnerError, assigned_topics::AssignedTopicsError,
    },
};

use super::{
    assignment_result::AssignedConsumerTryReplaceAssignmentErrorKind,
    result::AssignedConsumerPortError, shard::AssignedConsumerShardLockError,
};

pub(super) const fn assignment_error_kind(
    error: &AssignedConsumerPortError,
) -> AssignedConsumerTryReplaceAssignmentErrorKind {
    match error {
        AssignedConsumerPortError::Clock(error) => clock_error_kind(*error),
        AssignedConsumerPortError::Closed => AssignedConsumerTryReplaceAssignmentErrorKind::Closed,
        AssignedConsumerPortError::Lock(AssignedConsumerShardLockError::Contended) => {
            AssignedConsumerTryReplaceAssignmentErrorKind::Contended
        }
        AssignedConsumerPortError::Lock(
            AssignedConsumerShardLockError::Poisoned | AssignedConsumerShardLockError::OwnerMissing,
        ) => AssignedConsumerTryReplaceAssignmentErrorKind::HostUnavailable,
        AssignedConsumerPortError::Owner { error, .. } => owner_error_kind(*error),
    }
}

const fn owner_error_kind(
    error: AssignedConsumerOwnerError,
) -> AssignedConsumerTryReplaceAssignmentErrorKind {
    match error {
        AssignedConsumerOwnerError::Faulted => {
            AssignedConsumerTryReplaceAssignmentErrorKind::HostUnavailable
        }
        AssignedConsumerOwnerError::EffectsPending => {
            AssignedConsumerTryReplaceAssignmentErrorKind::Pending
        }
        AssignedConsumerOwnerError::DeliveryUnavailable
        | AssignedConsumerOwnerError::Close(_)
        | AssignedConsumerOwnerError::ControlInput(_) => {
            AssignedConsumerTryReplaceAssignmentErrorKind::InternalInvariant
        }
        AssignedConsumerOwnerError::Clock(error) => clock_error_kind(error),
        AssignedConsumerOwnerError::Topics(error) => topic_error_kind(error),
        AssignedConsumerOwnerError::Core(error) => core_error_kind(error),
        AssignedConsumerOwnerError::Completion(error) => completion_error_kind(error),
        AssignedConsumerOwnerError::Event(error) => event_error_kind(error),
        AssignedConsumerOwnerError::Allocation => {
            AssignedConsumerTryReplaceAssignmentErrorKind::ResourceExhausted
        }
    }
}

const fn clock_error_kind(error: ClockError) -> AssignedConsumerTryReplaceAssignmentErrorKind {
    match error {
        ClockError::InstantOverflow | ClockError::DeadlineOverflow => {
            AssignedConsumerTryReplaceAssignmentErrorKind::DeadlineOverflow
        }
        ClockError::BeforeEpoch | ClockError::TickOverflow => {
            AssignedConsumerTryReplaceAssignmentErrorKind::HostUnavailable
        }
    }
}

const fn topic_error_kind(
    error: AssignedTopicsError,
) -> AssignedConsumerTryReplaceAssignmentErrorKind {
    match error {
        AssignedTopicsError::PartitionCapacity { .. } => {
            AssignedConsumerTryReplaceAssignmentErrorKind::AssignmentCapacity
        }
        AssignedTopicsError::RetainedTopicCapacity { .. } => {
            AssignedConsumerTryReplaceAssignmentErrorKind::TopicCapacity
        }
        AssignedTopicsError::TopicNameBytes { .. }
        | AssignedTopicsError::RetainedNameBytes { .. } => {
            AssignedConsumerTryReplaceAssignmentErrorKind::RetainedNameCapacity
        }
        AssignedTopicsError::RetainedNameBytesOverflow
        | AssignedTopicsError::RetainedTopicCountOverflow
        | AssignedTopicsError::TopicIdentityExhausted => {
            AssignedConsumerTryReplaceAssignmentErrorKind::ResourceExhausted
        }
        AssignedTopicsError::UnknownTopic(_) => {
            AssignedConsumerTryReplaceAssignmentErrorKind::InternalInvariant
        }
    }
}

const fn core_error_kind(
    error: AssignedConsumerMachineError,
) -> AssignedConsumerTryReplaceAssignmentErrorKind {
    match error {
        AssignedConsumerMachineError::ConsumerClosed => {
            AssignedConsumerTryReplaceAssignmentErrorKind::Closed
        }
        AssignedConsumerMachineError::EmptyAssignment => {
            AssignedConsumerTryReplaceAssignmentErrorKind::EmptyAssignment
        }
        AssignedConsumerMachineError::DuplicatePartition { .. } => {
            AssignedConsumerTryReplaceAssignmentErrorKind::DuplicatePartition
        }
        AssignedConsumerMachineError::AssignmentEpochExhausted => {
            AssignedConsumerTryReplaceAssignmentErrorKind::ResourceExhausted
        }
        AssignedConsumerMachineError::CloseNotPending { .. }
        | AssignedConsumerMachineError::StaleClose { .. }
        | AssignedConsumerMachineError::CloseAlreadyCompleted { .. }
        | AssignedConsumerMachineError::AssignmentRetirementRejected { .. }
        | AssignedConsumerMachineError::NoAssignment
        | AssignedConsumerMachineError::StaleAssignment { .. }
        | AssignedConsumerMachineError::UnknownPartition { .. }
        | AssignedConsumerMachineError::PositionEpochExhausted { .. }
        | AssignedConsumerMachineError::FetchRevisionExhausted { .. }
        | AssignedConsumerMachineError::StalePosition { .. }
        | AssignedConsumerMachineError::PositionResolutionNotPending { .. }
        | AssignedConsumerMachineError::PositionResolutionDeadlineNotElapsed { .. }
        | AssignedConsumerMachineError::PositionThrottleNotPending { .. }
        | AssignedConsumerMachineError::PositionThrottleDeadlineNotElapsed { .. }
        | AssignedConsumerMachineError::FetchThrottleNotPending { .. }
        | AssignedConsumerMachineError::FetchThrottleDeadlineNotElapsed { .. }
        | AssignedConsumerMachineError::StaleFetch { .. }
        | AssignedConsumerMachineError::OffsetRegression { .. } => {
            AssignedConsumerTryReplaceAssignmentErrorKind::InternalInvariant
        }
    }
}

const fn completion_error_kind(
    error: CompletionRegistryError,
) -> AssignedConsumerTryReplaceAssignmentErrorKind {
    match error {
        CompletionRegistryError::Full | CompletionRegistryError::GenerationExhausted => {
            AssignedConsumerTryReplaceAssignmentErrorKind::ResourceExhausted
        }
        CompletionRegistryError::NotifierStopped | CompletionRegistryError::ReclaimDisconnected => {
            AssignedConsumerTryReplaceAssignmentErrorKind::HostUnavailable
        }
        CompletionRegistryError::UnknownCompletion
        | CompletionRegistryError::DuplicatePublish
        | CompletionRegistryError::NotificationBackpressure
        | CompletionRegistryError::UnsettledCompletion => {
            AssignedConsumerTryReplaceAssignmentErrorKind::InternalInvariant
        }
    }
}

const fn event_error_kind(
    error: AssignedConsumerEventStoreError,
) -> AssignedConsumerTryReplaceAssignmentErrorKind {
    match error {
        AssignedConsumerEventStoreError::Capacity => {
            AssignedConsumerTryReplaceAssignmentErrorKind::EventCapacity
        }
        AssignedConsumerEventStoreError::ClaimMissing
        | AssignedConsumerEventStoreError::ClaimMismatch
        | AssignedConsumerEventStoreError::TransitionMismatch => {
            AssignedConsumerTryReplaceAssignmentErrorKind::InternalInvariant
        }
    }
}
