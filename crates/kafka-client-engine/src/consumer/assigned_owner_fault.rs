//! Frozen linear recovery state for fatal assigned-owner mechanism failures.

use std::sync::Arc;

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerMachineError, AssignedConsumerTransition, PositionFence,
};

use crate::clock::ClockError;
use crate::completion::CompletionRegistryError;

use super::{
    assigned_close_error::AssignedCloseSlotError,
    assigned_event::AssignedConsumerEventStoreError,
    assigned_owner_model::PendingPosition,
    assigned_timer_model::AssignedTimerError,
    assigned_topics::AssignedTopicsError,
    fetch_execution::{FetchExecutionError, PrepareFetchError, PreparedFetchExecution},
    fetch_store::FetchDelivery,
    position_execution::PositionExecutionError,
    position_prepare_error::PreparePositionError,
};

/// Fatal ownership failure retained intact until post-driver-shutdown recovery.
#[allow(
    dead_code,
    clippy::large_enum_variant,
    reason = "fault payloads intentionally retain exact linear owners until shutdown recovery"
)]
pub(super) enum AssignedConsumerOwnerFault {
    Clock(ClockError),
    Effect {
        effect: AssignedConsumerEffect,
        failure: AssignedConsumerEffectFailure,
    },
    Event {
        effect: AssignedConsumerEffect,
        error: AssignedConsumerEventStoreError,
        topic: Arc<str>,
    },
    EventTransition {
        transition: AssignedConsumerTransition,
        error: AssignedConsumerEventStoreError,
    },
    Position(PositionExecutionError),
    Fetch(FetchExecutionError),
    Close(AssignedCloseSlotError),
    CloseCompletion(CompletionRegistryError),
    PendingPosition {
        error: AssignedConsumerMachineError,
        pending: PendingPosition,
    },
    PendingFetch {
        error: AssignedConsumerMachineError,
        pending: PreparedFetchExecution,
    },
    Delivery {
        error: AssignedConsumerMachineError,
        delivery: FetchDelivery,
    },
    Transition {
        transition: AssignedConsumerTransition,
        position_deadline: Option<crate::clock::OperationDeadline>,
    },
    Core {
        input: kafka_client_core::AssignedConsumerInput,
        error: AssignedConsumerMachineError,
    },
    UnexpectedTimerInput(kafka_client_core::AssignedConsumerInput),
}

/// Stable category without releasing any retained recovery payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedConsumerFaultKind {
    Clock,
    Effect,
    Event,
    Position,
    Fetch,
    Close,
    PendingPosition,
    PendingFetch,
    Delivery,
    Transition,
    Core,
    TimerInput,
    Reclaim,
}

impl AssignedConsumerOwnerFault {
    pub(super) fn kind(&self) -> AssignedConsumerFaultKind {
        match self {
            Self::Clock(_) => AssignedConsumerFaultKind::Clock,
            Self::Effect { .. } => AssignedConsumerFaultKind::Effect,
            Self::Event { .. } | Self::EventTransition { .. } => AssignedConsumerFaultKind::Event,
            Self::Position(_) => AssignedConsumerFaultKind::Position,
            Self::Fetch(_) => AssignedConsumerFaultKind::Fetch,
            Self::Close(_) | Self::CloseCompletion(_) => AssignedConsumerFaultKind::Close,
            Self::PendingPosition { .. } => AssignedConsumerFaultKind::PendingPosition,
            Self::PendingFetch { .. } => AssignedConsumerFaultKind::PendingFetch,
            Self::Delivery { .. } => AssignedConsumerFaultKind::Delivery,
            Self::Transition { .. } => AssignedConsumerFaultKind::Transition,
            Self::Core { .. } => AssignedConsumerFaultKind::Core,
            Self::UnexpectedTimerInput(_) => AssignedConsumerFaultKind::TimerInput,
        }
    }
}

/// Scalar reason the unchanged front effect could not be interpreted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AssignedConsumerEffectFailure {
    Clock(ClockError),
    Topic(AssignedTopicsError),
    PositionPreparation(PreparePositionError),
    FetchPreparation(PrepareFetchError),
    Timer(AssignedTimerError),
    Close(AssignedCloseSlotError),
    Event(AssignedConsumerEventStoreError),
    PendingCapacity,
    Allocation,
    PositionDeadlineMissing,
    PositionDeadlineMismatch {
        expected: PositionFence,
        supplied: PositionFence,
    },
}
