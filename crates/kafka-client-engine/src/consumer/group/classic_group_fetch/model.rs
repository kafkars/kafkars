//! Bounded first-slice policy, progress, and lossless Fetch preparation faults.

use kafka_client_core::{AssignedConsumerEffect, AssignedConsumerMachineError, Deadline};

use crate::{
    clock::ClockError,
    consumer::{
        assigned_event::AssignedConsumerEventStoreError,
        assigned_timer_model::AssignedTimerError,
        fetch_execution::{
            FetchAttemptDeadline, FetchExecutionError, PrepareFetchError, PreparedFetchExecution,
        },
    },
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
}

/// Fallible post-capture work that still retains the original attempt boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchCapturedFailure {
    Catalog(GroupSessionCatalogError),
    Preparation(PrepareFetchError),
}

/// Stable observation of every retained group Fetch fault shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchOwnerFaultKind {
    Activation(ClassicGroupFetchPostCoreFaultKind),
    Effect(ClassicGroupFetchEffectFailure),
    Captured(ClassicGroupFetchCapturedFailure),
    Pending(AssignedConsumerMachineError),
    Fetch(FetchExecutionError),
}

/// Full linear owner retained after an invariant or execution failure.
#[must_use = "group Fetch faults retain every exact linear owner until recovery"]
pub(in crate::consumer::group) enum ClassicGroupFetchOwnerFault {
    Activation(ClassicGroupFetchActivationFault),
    Effect {
        effect: AssignedConsumerEffect,
        failure: ClassicGroupFetchEffectFailure,
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
    Fetch(FetchExecutionError),
}

impl ClassicGroupFetchOwnerFault {
    pub(super) const fn kind(&self) -> ClassicGroupFetchOwnerFaultKind {
        match self {
            Self::Activation(fault) => ClassicGroupFetchOwnerFaultKind::Activation(fault.kind()),
            Self::Effect { failure, .. } => ClassicGroupFetchOwnerFaultKind::Effect(*failure),
            Self::Captured { failure, .. } => ClassicGroupFetchOwnerFaultKind::Captured(*failure),
            Self::Pending { error, .. } => ClassicGroupFetchOwnerFaultKind::Pending(*error),
            Self::Fetch(error) => ClassicGroupFetchOwnerFaultKind::Fetch(*error),
        }
    }

    #[cfg(test)]
    pub(super) const fn effect(&self) -> Option<AssignedConsumerEffect> {
        match self {
            Self::Effect { effect, .. } | Self::Captured { effect, .. } => Some(*effect),
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
