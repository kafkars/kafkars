//! Bytes-free KIP-848 lifecycle and failure vocabulary.

use super::ConsumerGroupHeartbeatAttempt;

/// Maximum partitions retained for one reviewed KIP-848 member assignment.
pub const CONSUMER_GROUP_HEARTBEAT_MAX_PARTITIONS: usize = 64;

/// Lifecycle phase of one deterministic KIP-848 member.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerGroupHeartbeatPhase {
    /// No heartbeat has entered membership ownership.
    Dormant,
    /// An initial or fenced-recovery epoch-zero Join heartbeat is outstanding.
    Joining,
    /// The broker accepted the member but has not supplied its first assignment.
    AwaitingAssignment,
    /// One broker assignment is live and a cadence deadline is armed.
    Stable,
    /// One assignment-fenced steady heartbeat is outstanding.
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
pub enum ConsumerGroupHeartbeatRequestKind {
    /// Epoch zero with the complete name-based subscription.
    Join,
    /// Current positive epoch with unchanged subscription and owned assignment.
    Steady,
    /// Epoch minus one, relinquishing the current member identity.
    Leave,
}

/// Stable terminal category normalized outside protocol and driver boundaries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerGroupHeartbeatFailure {
    /// The caller-owned absolute attempt deadline elapsed.
    DeadlineElapsed,
    /// Coordinator routing failed without a broker response.
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

/// Exact terminal KIP-848 attempt and its normalized cause.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerGroupHeartbeatFatal {
    attempt: ConsumerGroupHeartbeatAttempt,
    failure: ConsumerGroupHeartbeatFailure,
}

impl ConsumerGroupHeartbeatFatal {
    pub(crate) const fn new(
        attempt: ConsumerGroupHeartbeatAttempt,
        failure: ConsumerGroupHeartbeatFailure,
    ) -> Self {
        Self { attempt, failure }
    }

    /// Returns the exact terminal request identity.
    pub const fn attempt(self) -> ConsumerGroupHeartbeatAttempt {
        self.attempt
    }

    /// Returns the normalized terminal cause.
    pub const fn failure(self) -> ConsumerGroupHeartbeatFailure {
        self.failure
    }
}
