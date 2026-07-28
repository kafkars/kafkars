//! Closed facts, effects, and outcomes for sequential group-position reset.

use crate::{
    Deadline, Moment, NextFetchOffset, PositionResolutionAttemptFailure, StartPosition,
    consumer::group_commit::GroupAssignmentPartition,
};

use super::super::{GroupPositionBatch, GroupPositionFence};

/// Lifecycle stage for one bounded missing-offset reset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupPositionResetState {
    /// The retained reset has not emitted its first lookup.
    Ready,
    /// One exact missing partition awaits driver acceptance.
    AwaitingDriver,
    /// The driver owns the one current `ListOffsets` lookup.
    Submitted,
    /// Core assigned the sole terminal reset decision.
    Completed,
}

/// One normalized fact for sequential missing-offset resolution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupPositionResetInput {
    /// Starts the retained reset under its original bootstrap deadline.
    Start {
        /// Exact assignment fence authorizing the reset.
        fence: GroupPositionFence,
        /// Current monotonic observation.
        now: Moment,
    },
    /// The driver accepted the current partition lookup.
    DriverAccepted {
        /// Exact assignment fence carried by the accepted call.
        fence: GroupPositionFence,
        /// Exact partition now owned by the driver.
        partition: GroupAssignmentPartition,
    },
    /// The driver rejected the current lookup before transport ownership.
    DriverRejected {
        /// Exact assignment fence carried by the rejected request.
        fence: GroupPositionFence,
        /// Exact partition whose request was rejected.
        partition: GroupAssignmentPartition,
        /// Current monotonic terminal observation.
        now: Moment,
    },
    /// Kafka resolved the current missing partition.
    OffsetResolved {
        /// Exact assignment fence carried by the response.
        fence: GroupPositionFence,
        /// Exactly correlated assigned partition.
        partition: GroupAssignmentPartition,
        /// Current monotonic response observation.
        now: Moment,
        /// Nonnegative next offset to install.
        next_offset: NextFetchOffset,
        /// Nonnegative broker throttle from this response.
        throttle_time_ms: u32,
    },
    /// Driver, protocol, or broker execution failed.
    ResolutionFailed {
        /// Exact assignment fence carried by the terminal.
        fence: GroupPositionFence,
        /// Exactly correlated assigned partition.
        partition: GroupAssignmentPartition,
        /// Current monotonic terminal observation.
        now: Moment,
        /// Exact normalized failure category.
        failure: PositionResolutionAttemptFailure,
    },
    /// The original absolute bootstrap deadline elapsed.
    DeadlineElapsed {
        /// Exact assignment fence whose deadline elapsed.
        fence: GroupPositionFence,
        /// Current monotonic observation proving expiration.
        now: Moment,
    },
}

impl GroupPositionResetInput {
    pub(super) const fn fence(self) -> GroupPositionFence {
        match self {
            Self::Start { fence, .. }
            | Self::DriverAccepted { fence, .. }
            | Self::DriverRejected { fence, .. }
            | Self::OffsetResolved { fence, .. }
            | Self::ResolutionFailed { fence, .. }
            | Self::DeadlineElapsed { fence, .. } => fence,
        }
    }

    pub(super) const fn partition(self) -> Option<GroupAssignmentPartition> {
        match self {
            Self::DriverAccepted { partition, .. }
            | Self::DriverRejected { partition, .. }
            | Self::OffsetResolved { partition, .. }
            | Self::ResolutionFailed { partition, .. } => Some(partition),
            Self::Start { .. } | Self::DeadlineElapsed { .. } => None,
        }
    }
}

/// One concrete mechanism instruction from missing-offset reset policy.
#[derive(Debug, Eq, PartialEq)]
pub enum GroupPositionResetEffect {
    /// Resolve exactly one missing partition before considering the next.
    ResolveOffset {
        /// Exact assignment fence authorizing the lookup.
        fence: GroupPositionFence,
        /// Original absolute bootstrap deadline.
        deadline: Deadline,
        /// Exact assigned partition to query.
        partition: GroupAssignmentPartition,
        /// Kafka earliest-or-latest lookup policy.
        position: StartPosition,
    },
    /// Publish the sole core-owned reset terminal.
    Complete {
        /// Exact assignment fence owning the result.
        fence: GroupPositionFence,
        /// Original absolute bootstrap deadline.
        deadline: Deadline,
        /// Sole terminal reset decision.
        terminal: GroupPositionResetTerminal,
    },
}

/// Ordered result of one deterministic reset transition.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupPositionResetTransition {
    effect: Option<GroupPositionResetEffect>,
}

impl GroupPositionResetTransition {
    pub(super) const fn none() -> Self {
        Self { effect: None }
    }

    pub(super) const fn one(effect: GroupPositionResetEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<GroupPositionResetEffect> {
        self.effect
    }
}

/// Failed reset retaining the full assignment batch and exact failed partition.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupPositionResetFailure {
    batch: GroupPositionBatch,
    partition: GroupAssignmentPartition,
    failure: PositionResolutionAttemptFailure,
}

impl GroupPositionResetFailure {
    pub(super) const fn new(
        batch: GroupPositionBatch,
        partition: GroupAssignmentPartition,
        failure: PositionResolutionAttemptFailure,
    ) -> Self {
        Self {
            batch,
            partition,
            failure,
        }
    }

    /// Borrows all committed, resolved, and still-missing assignment facts.
    pub const fn batch(&self) -> &GroupPositionBatch {
        &self.batch
    }

    /// Returns the exact partition whose resolution failed.
    pub const fn partition(&self) -> GroupAssignmentPartition {
        self.partition
    }

    /// Returns the exact normalized terminal category.
    pub const fn failure(&self) -> PositionResolutionAttemptFailure {
        self.failure
    }

    /// Recovers the retained batch and failure facts.
    pub fn into_parts(
        self,
    ) -> (
        GroupPositionBatch,
        GroupAssignmentPartition,
        PositionResolutionAttemptFailure,
    ) {
        (self.batch, self.partition, self.failure)
    }
}

/// Sole terminal decision for one sequential reset.
#[derive(Debug, Eq, PartialEq)]
pub enum GroupPositionResetTerminal {
    /// Every missing assignment position was resolved.
    Ready(GroupPositionBatch),
    /// One exact lookup failed and later missing facts remain untouched.
    Failed(GroupPositionResetFailure),
}
