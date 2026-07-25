//! Positive rejoin timing and exact cycle or assignment-fenced schedules.

use crate::{AssignmentGeneration, Deadline};

use super::{ClassicBrokerError, ClassicBrokerStage, MembershipCycle};

/// Positive delay and attempt window for one internal classic rejoin.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClassicRejoinPolicy {
    backoff_ticks: u64,
    attempt_timeout_ticks: u64,
}

impl ClassicRejoinPolicy {
    /// Validates nonzero backoff and attempt timeout intervals.
    pub const fn try_new(
        backoff_ticks: u64,
        attempt_timeout_ticks: u64,
    ) -> Result<Self, ClassicRejoinPolicyError> {
        if backoff_ticks == 0 {
            return Err(ClassicRejoinPolicyError::ZeroBackoff);
        }
        if attempt_timeout_ticks == 0 {
            return Err(ClassicRejoinPolicyError::ZeroAttemptTimeout);
        }
        Ok(Self {
            backoff_ticks,
            attempt_timeout_ticks,
        })
    }

    /// Returns the positive delay before rejoining.
    pub const fn backoff_ticks(self) -> u64 {
        self.backoff_ticks
    }

    /// Returns the positive absolute-deadline window created when due.
    pub const fn attempt_timeout_ticks(self) -> u64 {
        self.attempt_timeout_ticks
    }
}

/// Invalid classic rejoin policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicRejoinPolicyError {
    /// Rejoin would be immediately runnable without an explicit positive delay.
    ZeroBackoff,
    /// A new membership cycle would have no positive attempt window.
    ZeroAttemptTimeout,
}

/// Whether the interpreter may retain or must rediscover its coordinator route.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicCoordinatorRecovery {
    /// The current coordinator observation remains usable.
    Retain,
    /// The current coordinator observation must be discarded and rediscovered.
    Rediscover,
}

/// Exact future rejoin fence created by one rejected cycle or assignment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClassicRejoinSchedule {
    cycle: MembershipCycle,
    assignment_generation: Option<AssignmentGeneration>,
    due: Deadline,
}

impl ClassicRejoinSchedule {
    pub(super) const fn new(
        cycle: MembershipCycle,
        assignment_generation: Option<AssignmentGeneration>,
        due: Deadline,
    ) -> Self {
        Self {
            cycle,
            assignment_generation,
            due,
        }
    }

    /// Returns the rejected membership cycle.
    pub const fn cycle(self) -> MembershipCycle {
        self.cycle
    }

    /// Returns the revoked assignment generation for heartbeat-originated loss.
    pub const fn assignment_generation(self) -> Option<AssignmentGeneration> {
        self.assignment_generation
    }

    /// Returns the exact absolute rejoin deadline.
    pub const fn due(self) -> Deadline {
        self.due
    }
}

/// Terminal state retained by one classic group machine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClassicGroupFatal {
    cycle: MembershipCycle,
    assignment_generation: Option<AssignmentGeneration>,
    reason: ClassicGroupFatalReason,
}

impl ClassicGroupFatal {
    pub(in crate::consumer::classic_group) const fn new(
        cycle: MembershipCycle,
        assignment_generation: Option<AssignmentGeneration>,
        reason: ClassicGroupFatalReason,
    ) -> Self {
        Self {
            cycle,
            assignment_generation,
            reason,
        }
    }

    /// Returns the membership cycle that entered fatal state.
    pub const fn cycle(self) -> MembershipCycle {
        self.cycle
    }

    /// Returns the revoked assignment generation when one was live.
    pub const fn assignment_generation(self) -> Option<AssignmentGeneration> {
        self.assignment_generation
    }

    /// Returns the exact terminal cause.
    pub const fn reason(self) -> ClassicGroupFatalReason {
        self.reason
    }
}

/// Why automatic classic membership recovery cannot continue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicGroupFatalReason {
    /// A stage rejected the request with this exact Kafka error.
    Broker {
        /// Stage that observed the broker rejection.
        stage: ClassicBrokerStage,
        /// Exact nonzero Kafka error code, including unknown future values.
        error: ClassicBrokerError,
    },
    /// Rejoin backoff could not produce an absolute due deadline.
    ScheduleDeadlineOverflow,
    /// No later nonreused membership cycle can be represented.
    CycleExhausted,
    /// A due rejoin could not produce its fresh absolute attempt deadline.
    AttemptDeadlineOverflow,
}
