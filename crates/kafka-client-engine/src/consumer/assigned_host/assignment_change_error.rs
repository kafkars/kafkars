//! Exhaustive translation from private incremental-assignment owners.

use kafka_client_core::AssignedConsumerMachineError;

use crate::{
    clock::ClockError,
    consumer::{
        assigned_event::AssignedConsumerEventStoreError,
        assigned_owner_model::AssignedConsumerOwnerError, assigned_topics::AssignedTopicsError,
    },
};

use super::{
    assignment_change_result::AssignedConsumerTryChangeAssignmentErrorKind as Kind,
    result::AssignedConsumerPortError, shard::AssignedConsumerShardLockError,
};

pub(super) const fn assignment_change_error_kind(error: &AssignedConsumerPortError) -> Kind {
    match error {
        AssignedConsumerPortError::Clock(error) => clock_error_kind(*error),
        AssignedConsumerPortError::Closed => Kind::Closed,
        AssignedConsumerPortError::Lock(AssignedConsumerShardLockError::Contended) => {
            Kind::Contended
        }
        AssignedConsumerPortError::Lock(
            AssignedConsumerShardLockError::Poisoned | AssignedConsumerShardLockError::OwnerMissing,
        ) => Kind::HostUnavailable,
        AssignedConsumerPortError::Owner { error, .. } => owner_error_kind(*error),
    }
}

const fn owner_error_kind(error: AssignedConsumerOwnerError) -> Kind {
    match error {
        AssignedConsumerOwnerError::Faulted => Kind::HostUnavailable,
        AssignedConsumerOwnerError::EffectsPending => Kind::Pending,
        AssignedConsumerOwnerError::Clock(error) => clock_error_kind(error),
        AssignedConsumerOwnerError::Topics(error) => topic_error_kind(error),
        AssignedConsumerOwnerError::Core(error) => core_error_kind(error),
        AssignedConsumerOwnerError::Event(error) => event_error_kind(error),
        AssignedConsumerOwnerError::Allocation => Kind::ResourceExhausted,
        AssignedConsumerOwnerError::DeliveryUnavailable
        | AssignedConsumerOwnerError::Close(_)
        | AssignedConsumerOwnerError::Completion(_)
        | AssignedConsumerOwnerError::ControlInput(_) => Kind::InternalInvariant,
    }
}

const fn clock_error_kind(error: ClockError) -> Kind {
    match error {
        ClockError::InstantOverflow | ClockError::DeadlineOverflow => Kind::DeadlineOverflow,
        ClockError::BeforeEpoch | ClockError::TickOverflow => Kind::HostUnavailable,
    }
}

const fn topic_error_kind(error: AssignedTopicsError) -> Kind {
    match error {
        AssignedTopicsError::PartitionCapacity { .. } => Kind::AssignmentCapacity,
        AssignedTopicsError::RetainedTopicCapacity { .. } => Kind::TopicCapacity,
        AssignedTopicsError::TopicNameBytes { .. }
        | AssignedTopicsError::RetainedNameBytes { .. } => Kind::RetainedNameCapacity,
        AssignedTopicsError::UnknownTopicName => Kind::UnknownPartition,
        AssignedTopicsError::RetainedNameBytesOverflow
        | AssignedTopicsError::RetainedTopicCountOverflow
        | AssignedTopicsError::TopicIdentityExhausted
        | AssignedTopicsError::Allocation => Kind::ResourceExhausted,
        AssignedTopicsError::UnknownTopic(_) => Kind::InternalInvariant,
    }
}

const fn core_error_kind(error: AssignedConsumerMachineError) -> Kind {
    match error {
        AssignedConsumerMachineError::ConsumerClosed => Kind::Closed,
        AssignedConsumerMachineError::NoAssignment => Kind::NoAssignment,
        AssignedConsumerMachineError::DuplicatePartition { .. } => Kind::DuplicatePartition,
        AssignedConsumerMachineError::PartitionAlreadyAssigned { .. } => Kind::AlreadyAssigned,
        AssignedConsumerMachineError::UnknownPartition { .. } => Kind::UnknownPartition,
        AssignedConsumerMachineError::ControlAllocationFailed
        | AssignedConsumerMachineError::AssignmentChangeAllocationFailed
        | AssignedConsumerMachineError::AssignmentEpochExhausted => Kind::ResourceExhausted,
        AssignedConsumerMachineError::CloseNotPending { .. }
        | AssignedConsumerMachineError::StaleClose { .. }
        | AssignedConsumerMachineError::CloseAlreadyCompleted { .. }
        | AssignedConsumerMachineError::AssignmentRetirementRejected { .. }
        | AssignedConsumerMachineError::EmptyAssignment
        | AssignedConsumerMachineError::StaleAssignment { .. }
        | AssignedConsumerMachineError::PositionEpochExhausted { .. }
        | AssignedConsumerMachineError::FetchRevisionExhausted { .. }
        | AssignedConsumerMachineError::PositionNotRetained { .. }
        | AssignedConsumerMachineError::StalePosition { .. }
        | AssignedConsumerMachineError::PositionResolutionNotPending { .. }
        | AssignedConsumerMachineError::PositionResolutionDeadlineNotElapsed { .. }
        | AssignedConsumerMachineError::PositionThrottleNotPending { .. }
        | AssignedConsumerMachineError::PositionThrottleDeadlineNotElapsed { .. }
        | AssignedConsumerMachineError::FetchThrottleNotPending { .. }
        | AssignedConsumerMachineError::FetchThrottleDeadlineNotElapsed { .. }
        | AssignedConsumerMachineError::StaleFetch { .. }
        | AssignedConsumerMachineError::OffsetRegression { .. } => Kind::InternalInvariant,
    }
}

const fn event_error_kind(error: AssignedConsumerEventStoreError) -> Kind {
    match error {
        AssignedConsumerEventStoreError::Capacity => Kind::EventCapacity,
        AssignedConsumerEventStoreError::ClaimMissing
        | AssignedConsumerEventStoreError::ClaimMismatch
        | AssignedConsumerEventStoreError::TransitionMismatch => Kind::InternalInvariant,
    }
}
