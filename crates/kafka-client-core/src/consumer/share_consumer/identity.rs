//! Nonreused identities fencing share membership, schedules, and retries.

use crate::{AssignmentGeneration, Deadline};

/// Positive member epoch issued by the share-group coordinator.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ShareGroupMemberEpoch(i32);

impl ShareGroupMemberEpoch {
    /// Accepts the positive stable-member epoch domain.
    pub const fn try_from_raw(value: i32) -> Option<Self> {
        if value > 0 { Some(Self(value)) } else { None }
    }

    /// Returns the exact Kafka member epoch.
    pub const fn get(self) -> i32 {
        self.0
    }
}

/// Nonreused identity of one `ShareGroupHeartbeat` request.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ShareGroupHeartbeatSequence(u64);

impl ShareGroupHeartbeatSequence {
    pub(super) const fn initial() -> Self {
        Self(1)
    }

    pub(super) const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Restores one validated nonzero heartbeat sequence.
    pub const fn try_from_raw(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the deterministic sequence value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Exact join, steady, or leave heartbeat identity.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ShareGroupHeartbeatAttempt {
    sequence: ShareGroupHeartbeatSequence,
    member_epoch: Option<ShareGroupMemberEpoch>,
}

impl ShareGroupHeartbeatAttempt {
    pub(super) const fn new(
        sequence: ShareGroupHeartbeatSequence,
        member_epoch: Option<ShareGroupMemberEpoch>,
    ) -> Self {
        Self {
            sequence,
            member_epoch,
        }
    }

    /// Returns the nonreused request sequence.
    pub const fn sequence(self) -> ShareGroupHeartbeatSequence {
        self.sequence
    }

    /// Returns the exact positive epoch, absent for an epoch-zero Join.
    pub const fn member_epoch(self) -> Option<ShareGroupMemberEpoch> {
        self.member_epoch
    }
}

/// Broker-controlled future heartbeat cadence fenced by its exact request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareGroupHeartbeatSchedule {
    attempt: ShareGroupHeartbeatAttempt,
    deadline: Deadline,
    assignment_generation: Option<AssignmentGeneration>,
}

impl ShareGroupHeartbeatSchedule {
    pub(super) const fn new(
        attempt: ShareGroupHeartbeatAttempt,
        deadline: Deadline,
        assignment_generation: Option<AssignmentGeneration>,
    ) -> Self {
        Self {
            attempt,
            deadline,
            assignment_generation,
        }
    }

    /// Returns the exact request identity armed by this schedule.
    pub const fn attempt(self) -> ShareGroupHeartbeatAttempt {
        self.attempt
    }

    /// Returns the broker-controlled cadence deadline.
    pub const fn deadline(self) -> Deadline {
        self.deadline
    }

    /// Returns the current local assignment fence, absent while awaiting one.
    pub const fn assignment_generation(self) -> Option<AssignmentGeneration> {
        self.assignment_generation
    }
}

/// Semantic authority paired with one positive retry delay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareGroupHeartbeatRetryCause {
    /// Kafka reported `COORDINATOR_LOAD_IN_PROGRESS` on the retained route.
    CoordinatorLoad,
    /// A stale share-coordinator route must be invalidated before replacement.
    Rediscovery,
}

/// Exact future fence for retrying one share heartbeat.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareGroupHeartbeatRetrySchedule {
    pub(super) attempt: ShareGroupHeartbeatAttempt,
    pub(super) kind: super::ShareGroupHeartbeatRequestKind,
    pub(super) cause: ShareGroupHeartbeatRetryCause,
    pub(super) not_before: Deadline,
    pub(super) deadline: Deadline,
}

impl ShareGroupHeartbeatRetrySchedule {
    /// Returns the exact request identity authorized after this delay.
    pub const fn attempt(self) -> ShareGroupHeartbeatAttempt {
        self.attempt
    }

    /// Returns the unchanged request shape.
    pub const fn kind(self) -> super::ShareGroupHeartbeatRequestKind {
        self.kind
    }

    /// Returns the exact fact authorizing this retry.
    pub const fn cause(self) -> ShareGroupHeartbeatRetryCause {
        self.cause
    }

    /// Returns the earliest absolute resubmission moment.
    pub const fn not_before(self) -> Deadline {
        self.not_before
    }

    /// Returns the original attempt deadline.
    pub const fn deadline(self) -> Deadline {
        self.deadline
    }
}
