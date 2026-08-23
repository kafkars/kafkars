//! Explicit deterministic rejection categories for share membership.

use core::fmt;

use crate::LiveGroupAssignmentError;

/// Why one normalized share-group fact could not change membership state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareGroupHeartbeatErrorKind {
    /// Membership has permanently closed.
    Closed,
    /// The input does not apply to the current lifecycle phase.
    InvalidPhase,
    /// The original or attempt deadline had already elapsed.
    DeadlineElapsed,
    /// A future absolute deadline could not be represented.
    DeadlineOverflow,
    /// Nonreused heartbeat identity space is exhausted.
    AttemptExhausted,
    /// Nonreused assignment-generation space is exhausted.
    AssignmentGenerationExhausted,
    /// The supplied request identity is not currently owned.
    AttemptMismatch,
    /// The normalized failure does not authorize rediscovery.
    FailureNotRetryable,
    /// The normalized failure is not exact coordinator loading.
    FailureNotCoordinatorLoad,
    /// The normalized failure does not authorize fenced-member recovery.
    FailureNotRecoverable,
    /// Another fact cannot settle while one retry schedule is retained.
    RetryPending,
    /// The supplied retry schedule is not currently owned.
    RetryScheduleMismatch,
    /// A retry observation arrived before its exact delay.
    RetryNotDue,
    /// The supplied cadence schedule is not currently owned.
    ScheduleMismatch,
    /// A cadence observation arrived before its exact deadline.
    ScheduleNotDue,
    /// The response member epoch regressed.
    MemberEpochRegression,
    /// The broker returned a zero heartbeat interval.
    ZeroHeartbeatInterval,
    /// The assignment exceeded the reviewed initial bound.
    AssignmentTooLarge,
    /// The normalized assignment was not ordered and unique.
    Assignment(LiveGroupAssignmentError),
    /// Retained state violates the machine ownership contract.
    InvariantViolation,
}

/// Public deterministic transition rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareGroupHeartbeatApplyError {
    kind: ShareGroupHeartbeatErrorKind,
}

impl ShareGroupHeartbeatApplyError {
    pub(super) const fn new(kind: ShareGroupHeartbeatErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable deterministic rejection category.
    pub const fn kind(self) -> ShareGroupHeartbeatErrorKind {
        self.kind
    }
}

impl fmt::Display for ShareGroupHeartbeatApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "share heartbeat transition rejected: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for ShareGroupHeartbeatApplyError {}
