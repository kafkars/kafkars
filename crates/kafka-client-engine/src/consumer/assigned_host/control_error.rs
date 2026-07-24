//! Exhaustive translation from private position owners to stable engine errors.

use kafka_client_core::AssignedConsumerMachineError;

use crate::{
    clock::ClockError,
    completion::CompletionRegistryError,
    consumer::{
        assigned_event::AssignedConsumerEventStoreError,
        assigned_owner_model::AssignedConsumerOwnerError,
    },
};

use super::{
    AssignedConsumerControlInputError, control_result::AssignedConsumerControlErrorKind,
    result::AssignedConsumerPortError, shard::AssignedConsumerShardLockError,
};

pub(super) const fn control_error_kind(
    error: &AssignedConsumerPortError,
) -> AssignedConsumerControlErrorKind {
    match error {
        AssignedConsumerPortError::Clock(error) => clock_error_kind(*error),
        AssignedConsumerPortError::Closed => AssignedConsumerControlErrorKind::Closed,
        AssignedConsumerPortError::Lock(AssignedConsumerShardLockError::Contended) => {
            AssignedConsumerControlErrorKind::Contended
        }
        AssignedConsumerPortError::Lock(
            AssignedConsumerShardLockError::Poisoned | AssignedConsumerShardLockError::OwnerMissing,
        ) => AssignedConsumerControlErrorKind::HostUnavailable,
        AssignedConsumerPortError::Owner { error, .. } => owner_error_kind(*error),
    }
}

const fn owner_error_kind(error: AssignedConsumerOwnerError) -> AssignedConsumerControlErrorKind {
    match error {
        AssignedConsumerOwnerError::Faulted
        | AssignedConsumerOwnerError::Completion(
            CompletionRegistryError::NotifierStopped
            | CompletionRegistryError::GenerationExhausted
            | CompletionRegistryError::ReclaimDisconnected,
        ) => AssignedConsumerControlErrorKind::HostUnavailable,
        AssignedConsumerOwnerError::EffectsPending => AssignedConsumerControlErrorKind::Pending,
        AssignedConsumerOwnerError::Core(error) => core_error_kind(error),
        AssignedConsumerOwnerError::Clock(error) => clock_error_kind(error),
        AssignedConsumerOwnerError::ControlInput(error) => control_input_error_kind(error),
        AssignedConsumerOwnerError::Event(AssignedConsumerEventStoreError::Capacity)
        | AssignedConsumerOwnerError::Allocation => {
            AssignedConsumerControlErrorKind::ResourceExhausted
        }
        AssignedConsumerOwnerError::DeliveryUnavailable
        | AssignedConsumerOwnerError::Topics(_)
        | AssignedConsumerOwnerError::Close(_)
        | AssignedConsumerOwnerError::Completion(_)
        | AssignedConsumerOwnerError::Event(_) => {
            AssignedConsumerControlErrorKind::InternalInvariant
        }
    }
}

const fn clock_error_kind(error: ClockError) -> AssignedConsumerControlErrorKind {
    match error {
        ClockError::InstantOverflow | ClockError::DeadlineOverflow => {
            AssignedConsumerControlErrorKind::DeadlineOverflow
        }
        ClockError::BeforeEpoch | ClockError::TickOverflow => {
            AssignedConsumerControlErrorKind::HostUnavailable
        }
    }
}

const fn control_input_error_kind(
    error: AssignedConsumerControlInputError,
) -> AssignedConsumerControlErrorKind {
    match error {
        AssignedConsumerControlInputError::UnknownTopic => {
            AssignedConsumerControlErrorKind::UnknownPartition
        }
        AssignedConsumerControlInputError::NegativeOffset => {
            AssignedConsumerControlErrorKind::NegativeOffset
        }
    }
}

const fn core_error_kind(error: AssignedConsumerMachineError) -> AssignedConsumerControlErrorKind {
    match error {
        AssignedConsumerMachineError::ConsumerClosed => AssignedConsumerControlErrorKind::Closed,
        AssignedConsumerMachineError::NoAssignment => {
            AssignedConsumerControlErrorKind::NoAssignment
        }
        AssignedConsumerMachineError::StaleAssignment { .. } => {
            AssignedConsumerControlErrorKind::StaleAssignment
        }
        AssignedConsumerMachineError::UnknownPartition { .. } => {
            AssignedConsumerControlErrorKind::UnknownPartition
        }
        AssignedConsumerMachineError::AssignmentEpochExhausted
        | AssignedConsumerMachineError::PositionEpochExhausted { .. }
        | AssignedConsumerMachineError::FetchRevisionExhausted { .. } => {
            AssignedConsumerControlErrorKind::ResourceExhausted
        }
        AssignedConsumerMachineError::CloseNotPending { .. }
        | AssignedConsumerMachineError::StaleClose { .. }
        | AssignedConsumerMachineError::CloseAlreadyCompleted { .. }
        | AssignedConsumerMachineError::EmptyAssignment
        | AssignedConsumerMachineError::DuplicatePartition { .. }
        | AssignedConsumerMachineError::StalePosition { .. }
        | AssignedConsumerMachineError::PositionResolutionNotPending { .. }
        | AssignedConsumerMachineError::PositionResolutionDeadlineNotElapsed { .. }
        | AssignedConsumerMachineError::PositionThrottleNotPending { .. }
        | AssignedConsumerMachineError::PositionThrottleDeadlineNotElapsed { .. }
        | AssignedConsumerMachineError::FetchThrottleNotPending { .. }
        | AssignedConsumerMachineError::FetchThrottleDeadlineNotElapsed { .. }
        | AssignedConsumerMachineError::StaleFetch { .. }
        | AssignedConsumerMachineError::OffsetRegression { .. } => {
            AssignedConsumerControlErrorKind::InternalInvariant
        }
    }
}
