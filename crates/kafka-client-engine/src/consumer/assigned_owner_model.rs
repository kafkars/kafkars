//! Immutable bounds and lossless outcomes for one assigned-consumer owner.

use std::time::Duration;

use kafka_client_core::{AssignedConsumerMachineError, Deadline, PositionFence};

use crate::{
    clock::ClockError,
    completion::CompletionRegistryError,
    protocol::{
        consumer::ListOffsetsIsolation,
        fetch::{FetchDecodeLimits, FetchRequestSettings},
    },
};

use super::{
    assigned_close_error::AssignedCloseSlotError,
    assigned_event::{AssignedConsumerEventStoreBuildError, AssignedConsumerEventStoreError},
    assigned_host::AssignedConsumerControlInputError,
    assigned_topics::{AssignedTopicLimits, AssignedTopicsError},
    position_execution::PreparedPositionResolution,
};

/// Immutable resource limits for one direct-assignment lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct AssignedConsumerOwnerLimits {
    pub(super) partition_capacity: usize,
    pub(super) effect_capacity: usize,
    pub(super) call_capacity: usize,
    pub(super) delivery_capacity: usize,
    pub(super) delivery_bytes: usize,
    pub(super) hard_fetch_output_bytes: usize,
    pub(super) topic_limits: AssignedTopicLimits,
}

impl AssignedConsumerOwnerLimits {
    #[allow(
        clippy::too_many_arguments,
        reason = "the owner must receive every independent resource bound explicitly"
    )]
    pub(super) fn new(
        partition_capacity: usize,
        call_capacity: usize,
        delivery_capacity: usize,
        delivery_bytes: usize,
        hard_fetch_output_bytes: usize,
        topic_limits: AssignedTopicLimits,
    ) -> Result<Self, AssignedConsumerOwnerBuildError> {
        if partition_capacity == 0 {
            return Err(AssignedConsumerOwnerBuildError::ZeroPartitionCapacity);
        }
        if call_capacity == 0 {
            return Err(AssignedConsumerOwnerBuildError::ZeroCallCapacity);
        }
        if delivery_capacity == 0 || delivery_bytes == 0 || hard_fetch_output_bytes == 0 {
            return Err(AssignedConsumerOwnerBuildError::ZeroDeliveryCapacity);
        }
        if topic_limits.max_partitions() > partition_capacity {
            return Err(AssignedConsumerOwnerBuildError::TopicPartitionCapacity {
                topic: topic_limits.max_partitions(),
                owner: partition_capacity,
            });
        }
        if hard_fetch_output_bytes > delivery_bytes {
            return Err(AssignedConsumerOwnerBuildError::FetchOutputBytes {
                actual: hard_fetch_output_bytes,
                limit: delivery_bytes,
            });
        }
        let effect_capacity = partition_capacity
            .checked_mul(2)
            .and_then(|value| value.checked_add(1))
            .ok_or(AssignedConsumerOwnerBuildError::CapacityOverflow)?;
        Ok(Self {
            partition_capacity,
            effect_capacity,
            call_capacity,
            delivery_capacity,
            delivery_bytes,
            hard_fetch_output_bytes,
            topic_limits,
        })
    }
}

/// Immutable execution policy already compiled outside this owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct AssignedConsumerOwnerSettings {
    pub(super) position_isolation: ListOffsetsIsolation,
    pub(super) fetch_settings: FetchRequestSettings,
    pub(super) fetch_decode_limits: FetchDecodeLimits,
    pub(super) fetch_attempt_timeout: Duration,
    pub(super) due_timer_budget: usize,
}

impl AssignedConsumerOwnerSettings {
    pub(super) const fn new(
        position_isolation: ListOffsetsIsolation,
        fetch_settings: FetchRequestSettings,
        fetch_decode_limits: FetchDecodeLimits,
        fetch_attempt_timeout: Duration,
        due_timer_budget: usize,
    ) -> Self {
        Self {
            position_isolation,
            fetch_settings,
            fetch_decode_limits,
            fetch_attempt_timeout,
            due_timer_budget,
        }
    }
}

/// Failure before the owner exists; no hidden allocation fallback is used.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedConsumerOwnerBuildError {
    CapacityOverflow,
    ZeroTimerBudget,
    ZeroPartitionCapacity,
    ZeroCallCapacity,
    ZeroDeliveryCapacity,
    TopicPartitionCapacity { topic: usize, owner: usize },
    FetchOutputBytes { actual: usize, limit: usize },
    Event(AssignedConsumerEventStoreBuildError),
    Allocation,
}

/// Rejection at a caller-visible owner boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedConsumerOwnerError {
    Faulted,
    EffectsPending,
    DeliveryUnavailable,
    Clock(ClockError),
    Topics(AssignedTopicsError),
    Core(AssignedConsumerMachineError),
    Close(AssignedCloseSlotError),
    Completion(CompletionRegistryError),
    Event(AssignedConsumerEventStoreError),
    ControlInput(AssignedConsumerControlInputError),
    Allocation,
}

/// Exact operation deadline retained beside a raw position effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RawPositionDeadline {
    pub(super) fence: PositionFence,
    pub(super) deadline: crate::clock::OperationDeadline,
}

/// Linear prepared position work paired with its original absolute deadline.
pub(super) struct PendingPosition {
    pub(super) prepared: PreparedPositionResolution,
    pub(super) deadline: crate::clock::OperationDeadline,
}

/// Bounded work performed by one deterministic owner turn.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "this concrete report exposes independent bounded turn stages"
)]
pub(crate) struct AssignedConsumerTurn {
    pub(crate) effect_interpreted: bool,
    pub(crate) timer_inputs: usize,
    pub(crate) position_polled: bool,
    pub(crate) fetch_polled: bool,
    pub(crate) position_submitted: bool,
    pub(crate) fetch_submitted: bool,
    pub(crate) close_progressed: bool,
}

impl AssignedConsumerTurn {
    /// Reports only work that this exact bounded turn committed or consumed.
    pub(crate) const fn progressed(self) -> bool {
        self.effect_interpreted
            || self.timer_inputs != 0
            || self.position_polled
            || self.fetch_polled
            || self.position_submitted
            || self.fetch_submitted
            || self.close_progressed
    }
}

pub(super) fn minimum_deadline(current: Option<Deadline>, candidate: Deadline) -> Option<Deadline> {
    Some(current.map_or(candidate, |present| present.min(candidate)))
}
