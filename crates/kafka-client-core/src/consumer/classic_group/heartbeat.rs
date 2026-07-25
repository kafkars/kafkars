//! Positive heartbeat timing and assignment-fenced cadence ownership.

use crate::{AssignmentGeneration, Deadline};

use super::MembershipCycle;

/// Nonzero heartbeat sequence within one nonreused assignment.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClassicHeartbeatSequence(u64);

impl ClassicHeartbeatSequence {
    const fn initial() -> Self {
        Self(1)
    }

    const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the one-based sequence value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Exact assignment and sequence fence for one classic heartbeat.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClassicHeartbeatAttempt {
    cycle: MembershipCycle,
    assignment_generation: AssignmentGeneration,
    sequence: ClassicHeartbeatSequence,
}

impl ClassicHeartbeatAttempt {
    pub(super) const fn first(
        cycle: MembershipCycle,
        assignment_generation: AssignmentGeneration,
    ) -> Self {
        Self {
            cycle,
            assignment_generation,
            sequence: ClassicHeartbeatSequence::initial(),
        }
    }

    pub(super) const fn checked_next(self) -> Option<Self> {
        match self.sequence.checked_next() {
            Some(sequence) => Some(Self { sequence, ..self }),
            None => None,
        }
    }

    /// Returns the nonreused membership cycle.
    pub const fn cycle(self) -> MembershipCycle {
        self.cycle
    }

    /// Returns the core-owned live-assignment generation.
    pub const fn assignment_generation(self) -> AssignmentGeneration {
        self.assignment_generation
    }

    /// Returns the one-based sequence within this assignment.
    pub const fn sequence(self) -> ClassicHeartbeatSequence {
        self.sequence
    }
}

/// Exact future heartbeat fence and due deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClassicHeartbeatSchedule {
    attempt: ClassicHeartbeatAttempt,
    due: Deadline,
    liveness_deadline: Deadline,
}

impl ClassicHeartbeatSchedule {
    pub(super) const fn new(
        attempt: ClassicHeartbeatAttempt,
        due: Deadline,
        liveness_deadline: Deadline,
    ) -> Self {
        Self {
            attempt,
            due,
            liveness_deadline,
        }
    }

    /// Returns the assignment-fenced heartbeat identity.
    pub const fn attempt(self) -> ClassicHeartbeatAttempt {
        self.attempt
    }

    /// Returns the exact absolute cadence deadline.
    pub const fn due(self) -> Deadline {
        self.due
    }

    /// Returns the conservative deadline after which assignment is not claimed.
    pub const fn liveness_deadline(self) -> Deadline {
        self.liveness_deadline
    }

    /// Returns the first exact deadline the engine must observe.
    pub const fn next_deadline(self) -> Deadline {
        self.due.min(self.liveness_deadline)
    }
}
