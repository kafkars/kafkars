//! Directional identities fencing KIP-848 member epochs and heartbeat attempts.

use crate::{AssignmentGeneration, Deadline};

/// Positive member epoch issued by Kafka's consumer-group coordinator.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConsumerGroupMemberEpoch(i32);

impl ConsumerGroupMemberEpoch {
    /// Accepts the positive stable-member epoch domain.
    pub const fn try_from_raw(value: i32) -> Option<Self> {
        if value > 0 { Some(Self(value)) } else { None }
    }

    /// Returns the exact Kafka member epoch.
    pub const fn get(self) -> i32 {
        self.0
    }
}

/// Nonreused identity of one ConsumerGroupHeartbeat request.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConsumerGroupHeartbeatSequence(u64);

impl ConsumerGroupHeartbeatSequence {
    pub(crate) const fn initial() -> Self {
        Self(1)
    }

    pub(crate) const fn checked_next(self) -> Option<Self> {
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

/// Exact join or member heartbeat request identity.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ConsumerGroupHeartbeatAttempt {
    sequence: ConsumerGroupHeartbeatSequence,
    member_epoch: Option<ConsumerGroupMemberEpoch>,
}

impl ConsumerGroupHeartbeatAttempt {
    pub(crate) const fn new(
        sequence: ConsumerGroupHeartbeatSequence,
        member_epoch: Option<ConsumerGroupMemberEpoch>,
    ) -> Self {
        Self {
            sequence,
            member_epoch,
        }
    }

    /// Returns the nonreused request sequence.
    pub const fn sequence(self) -> ConsumerGroupHeartbeatSequence {
        self.sequence
    }

    /// Returns the exact stable epoch sent by this attempt, absent for initial join.
    pub const fn member_epoch(self) -> Option<ConsumerGroupMemberEpoch> {
        self.member_epoch
    }
}

/// Broker-controlled future heartbeat cadence fenced by its exact request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerGroupHeartbeatSchedule {
    attempt: ConsumerGroupHeartbeatAttempt,
    deadline: Deadline,
    assignment_generation: AssignmentGeneration,
}

impl ConsumerGroupHeartbeatSchedule {
    pub(crate) const fn new(
        attempt: ConsumerGroupHeartbeatAttempt,
        deadline: Deadline,
        assignment_generation: AssignmentGeneration,
    ) -> Self {
        Self {
            attempt,
            deadline,
            assignment_generation,
        }
    }

    /// Returns the exact request identity armed by this schedule.
    pub const fn attempt(self) -> ConsumerGroupHeartbeatAttempt {
        self.attempt
    }

    /// Returns the broker-controlled cadence deadline.
    pub const fn deadline(self) -> Deadline {
        self.deadline
    }

    /// Returns the assignment generation whose owned partitions will be reported.
    pub const fn assignment_generation(self) -> AssignmentGeneration {
        self.assignment_generation
    }
}
