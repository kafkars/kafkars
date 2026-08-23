//! Bytes-free share-group lifecycle, policy, and failure vocabulary.

use super::ShareGroupHeartbeatAttempt;

/// Maximum partitions retained for one initial share-consumer assignment.
pub const SHARE_GROUP_HEARTBEAT_MAX_PARTITIONS: usize = 64;

/// Lifecycle phase of one deterministic share member.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareGroupHeartbeatPhase {
    /// No heartbeat has entered membership ownership.
    Dormant,
    /// An epoch-zero Join heartbeat is outstanding.
    Joining,
    /// Kafka accepted membership but has not supplied an assignment.
    AwaitingAssignment,
    /// One assignment is current and a cadence deadline is armed.
    Stable,
    /// One current-epoch heartbeat is outstanding.
    Heartbeating,
    /// One epoch-minus-one leave heartbeat is outstanding.
    Leaving,
    /// Membership ended with an exact terminal cause.
    Fatal,
    /// Admission and membership are permanently closed.
    Closed,
}

/// Request shape selected by deterministic membership state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareGroupHeartbeatRequestKind {
    /// Epoch zero with the complete subscription.
    Join,
    /// Current positive epoch with an unchanged subscription.
    Steady,
    /// Epoch minus one, relinquishing the stable member identity.
    Leave,
}

/// Stable terminal category normalized outside protocol and driver boundaries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareGroupHeartbeatFailure {
    /// The current absolute attempt deadline elapsed.
    DeadlineElapsed,
    /// Share membership coordinator routing failed without a broker response.
    CoordinatorUnavailable,
    /// The broker and client share no supported heartbeat version.
    Compatibility,
    /// Transport or driver execution ended terminally.
    Execution,
    /// Kafka returned one exact nonzero signed error code.
    Broker(i16),
    /// A successful response was malformed or exceeded bounds.
    InvalidResponse,
}

/// Exact terminal attempt and normalized cause.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareGroupHeartbeatFatal {
    attempt: ShareGroupHeartbeatAttempt,
    failure: ShareGroupHeartbeatFailure,
}

impl ShareGroupHeartbeatFatal {
    pub(super) const fn new(
        attempt: ShareGroupHeartbeatAttempt,
        failure: ShareGroupHeartbeatFailure,
    ) -> Self {
        Self { attempt, failure }
    }

    /// Returns the exact terminal request identity.
    pub const fn attempt(self) -> ShareGroupHeartbeatAttempt {
        self.attempt
    }

    /// Returns the normalized terminal cause.
    pub const fn failure(self) -> ShareGroupHeartbeatFailure {
        self.failure
    }
}

/// Immutable local timeout for steady share heartbeats.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareGroupHeartbeatPolicy {
    attempt_timeout_ticks: u64,
}

impl ShareGroupHeartbeatPolicy {
    /// Creates a positive heartbeat-attempt timeout.
    pub const fn try_new(
        attempt_timeout_ticks: u64,
    ) -> Result<Self, ShareGroupHeartbeatPolicyError> {
        if attempt_timeout_ticks == 0 {
            Err(ShareGroupHeartbeatPolicyError::ZeroAttemptTimeout)
        } else {
            Ok(Self {
                attempt_timeout_ticks,
            })
        }
    }

    /// Returns the local attempt timeout in deterministic ticks.
    pub const fn attempt_timeout_ticks(self) -> u64 {
        self.attempt_timeout_ticks
    }
}

/// Invalid share heartbeat policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareGroupHeartbeatPolicyError {
    /// A zero timeout cannot bound an admitted attempt.
    ZeroAttemptTimeout,
}
