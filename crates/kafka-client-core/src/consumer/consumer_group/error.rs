//! Explicit deterministic rejection categories for KIP-848 transitions.

use core::fmt;

use crate::LiveGroupAssignmentError;

/// Why one normalized KIP-848 fact could not change membership state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerGroupHeartbeatErrorKind {
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
    /// The normalized failure does not authorize a coordinator-rediscovery replacement.
    FailureNotRetryable,
    /// The normalized failure does not authorize a fenced-membership recovery Join.
    FailureNotRecoverable,
    /// The normalized failure is not exact `COORDINATOR_LOAD_IN_PROGRESS`.
    FailureNotCoordinatorLoad,
    /// Another fact cannot settle the request while its coordinator-load retry is waiting.
    CoordinatorLoadRetryPending,
    /// The supplied coordinator-load retry schedule is not currently owned.
    CoordinatorLoadRetryScheduleMismatch,
    /// A coordinator-load retry observation arrived before its exact backoff deadline.
    CoordinatorLoadRetryNotDue,
    /// The supplied cadence schedule is not currently owned.
    ScheduleMismatch,
    /// A cadence observation arrived before its exact deadline.
    ScheduleNotDue,
    /// The response named another member identity.
    MemberMismatch,
    /// The response member epoch regressed.
    MemberEpochRegression,
    /// A pending reconciliation target changed without a member-epoch change.
    AssignmentChangedWithoutEpoch,
    /// A newer broker member epoch replaced an unfinished reconciliation target.
    ReconciliationEpochChanged,
    /// Retirement did not match the exact pending member and current assignment fence.
    ReconciliationMismatch,
    /// Initial join did not provide an assignment.
    InitialAssignmentMissing,
    /// The broker returned a zero heartbeat interval.
    ZeroHeartbeatInterval,
    /// The assignment exceeded the reviewed first-beta partition bound.
    AssignmentTooLarge,
    /// The normalized assignment was not ordered and unique.
    Assignment(LiveGroupAssignmentError),
    /// Retained state violates the machine's ownership contract.
    InvariantViolation,
}

/// Public deterministic transition rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerGroupHeartbeatApplyError {
    kind: ConsumerGroupHeartbeatErrorKind,
}

impl ConsumerGroupHeartbeatApplyError {
    pub(crate) const fn new(kind: ConsumerGroupHeartbeatErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable deterministic rejection category.
    pub const fn kind(self) -> ConsumerGroupHeartbeatErrorKind {
        self.kind
    }
}

impl fmt::Display for ConsumerGroupHeartbeatApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "KIP-848 heartbeat transition rejected: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for ConsumerGroupHeartbeatApplyError {}
