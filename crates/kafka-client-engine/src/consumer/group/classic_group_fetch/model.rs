//! Bounded first-slice policy, progress, and lossless Fetch preparation faults.

use crate::{
    clock::ClockError,
    consumer::{
        assigned_event::{AssignedConsumerEvent, AssignedConsumerEventStoreError},
        assigned_owner_model::PendingPosition,
        assigned_timer_model::AssignedTimerError,
        fetch_execution::{
            FetchAttemptDeadline, FetchExecutionError, FetchReclaimFailure,
            PartitionOffsetOutOfRangeProposal, PrepareFetchError, PreparedFetchExecution,
        },
        position_execution::PositionExecutionError,
        position_prepare_error::PreparePositionError,
    },
};
use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachineError,
    AssignedConsumerTransition, Deadline,
};

use super::{
    super::session_catalog::GroupSessionCatalogError,
    activation::{ClassicGroupFetchActivationFault, ClassicGroupFetchPostCoreFaultKind},
};

/// Failure before one bounded group Fetch owner exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchBuildError {
    Allocation,
}

/// Lossless pre-core activation rejection after the resolved input was copied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchPreflightError {
    EffectCapacity { actual: usize, limit: usize },
    PreparedCapacity { actual: usize, limit: usize },
    Event(AssignedConsumerEventStoreError),
}

/// Result of interpreting at most one exact front effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchFront {
    Idle,
    Interpreted,
    Backpressured,
    ControlPending,
}

/// Stable reason one exact front effect could not be interpreted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchEffectFailure {
    Clock(ClockError),
    Timer(AssignedTimerError),
    Event(AssignedConsumerEventStoreError),
    TerminalCatalog(GroupSessionCatalogError),
    PositionCatalog(GroupSessionCatalogError),
    PositionPreparation(PreparePositionError),
    PositionCapacity,
    PositionDeadlineMismatch,
}

/// Fallible post-capture work that still retains the original attempt boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchCapturedFailure {
    Catalog(GroupSessionCatalogError),
    Preparation(PrepareFetchError),
}

/// Stable reason an already-applied transition could not enter the effect FIFO.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchTransitionFailure {
    ControlInvariant,
    EffectCapacity { actual: usize, limit: usize },
    RetirementControls,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchOffsetResetFailure {
    Clock(ClockError),
    EffectCapacity { actual: usize, limit: usize },
    RawDeadlineCapacity { actual: usize, limit: usize },
    PendingPositionCapacity { actual: usize, limit: usize },
    Event(AssignedConsumerEventStoreError),
}

/// Stable observation of every retained group Fetch fault shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchOwnerFaultKind {
    Activation(ClassicGroupFetchPostCoreFaultKind),
    Clock(ClockError),
    Effect(ClassicGroupFetchEffectFailure),
    Captured(ClassicGroupFetchCapturedFailure),
    Pending(AssignedConsumerMachineError),
    PendingPosition(AssignedConsumerMachineError),
    Position,
    Core(AssignedConsumerMachineError),
    Transition(ClassicGroupFetchTransitionFailure),
    TimerInput,
    Fetch(FetchExecutionError),
    Delivery(AssignedConsumerMachineError),
    DeliveryCatalog(GroupSessionCatalogError),
    DeliveryPartition,
    DeliveryEvent(Option<AssignedConsumerMachineError>),
    Reclaim,
    Reconciliation(super::reconciliation::ClassicGroupFetchReconciliationErrorKind),
    OffsetReset(ClassicGroupFetchOffsetResetFailure),
}

/// Full linear owner retained after an invariant or execution failure.
#[must_use = "group Fetch faults retain every exact linear owner until recovery"]
#[allow(
    dead_code,
    clippy::large_enum_variant,
    reason = "fault variants retain exact linear owners for later shutdown recovery"
)]
pub(in crate::consumer::group) enum ClassicGroupFetchOwnerFault {
    Activation(ClassicGroupFetchActivationFault),
    Clock(ClockError),
    Effect {
        effect: AssignedConsumerEffect,
        failure: ClassicGroupFetchEffectFailure,
    },
    Event {
        effect: AssignedConsumerEffect,
        error: AssignedConsumerEventStoreError,
        _topic: std::sync::Arc<str>,
    },
    Captured {
        effect: AssignedConsumerEffect,
        attempt: FetchAttemptDeadline,
        failure: ClassicGroupFetchCapturedFailure,
    },
    Pending {
        error: AssignedConsumerMachineError,
        _prepared: PreparedFetchExecution,
    },
    PendingPosition {
        error: AssignedConsumerMachineError,
        _pending: PendingPosition,
    },
    Position(PositionExecutionError),
    Core {
        _input: AssignedConsumerInput,
        error: AssignedConsumerMachineError,
    },
    Transition {
        _transition: AssignedConsumerTransition,
        failure: ClassicGroupFetchTransitionFailure,
    },
    UnexpectedTimerInput {
        _input: AssignedConsumerInput,
    },
    Fetch(FetchExecutionError),
    Delivery {
        error: AssignedConsumerMachineError,
        _delivery: crate::consumer::fetch_store::FetchDelivery,
    },
    DeliveryCatalog {
        error: GroupSessionCatalogError,
        _delivery: crate::consumer::fetch_store::FetchDelivery,
    },
    DeliveryPartition {
        _delivery: crate::consumer::fetch_store::FetchDelivery,
    },
    DeliveryEvent {
        error: Option<AssignedConsumerMachineError>,
        _event: AssignedConsumerEvent,
    },
    Reclaim {
        _failure: FetchReclaimFailure,
    },
    Reconciliation {
        _completed: super::super::classic_group_position::ClassicGroupPositionCompleted,
        transition: AssignedConsumerTransition,
        kind: super::reconciliation::ClassicGroupFetchReconciliationErrorKind,
    },
    OffsetReset {
        _proposal: PartitionOffsetOutOfRangeProposal,
        failure: ClassicGroupFetchOffsetResetFailure,
    },
}

impl ClassicGroupFetchOwnerFault {
    pub(in crate::consumer::group) const fn kind(&self) -> ClassicGroupFetchOwnerFaultKind {
        match self {
            Self::Activation(fault) => ClassicGroupFetchOwnerFaultKind::Activation(fault.kind()),
            Self::Clock(error) => ClassicGroupFetchOwnerFaultKind::Clock(*error),
            Self::Effect { failure, .. } => ClassicGroupFetchOwnerFaultKind::Effect(*failure),
            Self::Event { error, .. } => ClassicGroupFetchOwnerFaultKind::Effect(
                ClassicGroupFetchEffectFailure::Event(*error),
            ),
            Self::Captured { failure, .. } => ClassicGroupFetchOwnerFaultKind::Captured(*failure),
            Self::Pending { error, .. } => ClassicGroupFetchOwnerFaultKind::Pending(*error),
            Self::PendingPosition { error, .. } => {
                ClassicGroupFetchOwnerFaultKind::PendingPosition(*error)
            }
            Self::Position(_) => ClassicGroupFetchOwnerFaultKind::Position,
            Self::Core { error, .. } => ClassicGroupFetchOwnerFaultKind::Core(*error),
            Self::Transition { failure, .. } => {
                ClassicGroupFetchOwnerFaultKind::Transition(*failure)
            }
            Self::UnexpectedTimerInput { .. } => ClassicGroupFetchOwnerFaultKind::TimerInput,
            Self::Fetch(error) => ClassicGroupFetchOwnerFaultKind::Fetch(*error),
            Self::Delivery { error, .. } => ClassicGroupFetchOwnerFaultKind::Delivery(*error),
            Self::DeliveryCatalog { error, .. } => {
                ClassicGroupFetchOwnerFaultKind::DeliveryCatalog(*error)
            }
            Self::DeliveryPartition { .. } => ClassicGroupFetchOwnerFaultKind::DeliveryPartition,
            Self::DeliveryEvent { error, .. } => {
                ClassicGroupFetchOwnerFaultKind::DeliveryEvent(*error)
            }
            Self::Reclaim { .. } => ClassicGroupFetchOwnerFaultKind::Reclaim,
            Self::Reconciliation { kind, .. } => {
                ClassicGroupFetchOwnerFaultKind::Reconciliation(*kind)
            }
            Self::OffsetReset { failure, .. } => {
                ClassicGroupFetchOwnerFaultKind::OffsetReset(*failure)
            }
        }
    }

    #[cfg(test)]
    pub(super) const fn effect(&self) -> Option<AssignedConsumerEffect> {
        match self {
            Self::Effect { effect, .. }
            | Self::Event { effect, .. }
            | Self::Captured { effect, .. } => Some(*effect),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(super) const fn captured_attempt(&self) -> Option<&FetchAttemptDeadline> {
        match self {
            Self::Captured { attempt, .. } => Some(attempt),
            _ => None,
        }
    }
}

pub(super) fn minimum_deadline(current: Option<Deadline>, candidate: Deadline) -> Option<Deadline> {
    Some(current.map_or(candidate, |present| present.min(candidate)))
}
