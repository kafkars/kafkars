//! Assignment-fenced application-processing liveness for one classic member.

use crate::{AssignmentGeneration, Deadline, GroupId, Moment};

use super::MembershipCycle;

#[path = "processing_lease/machine.rs"]
mod machine;
pub use machine::{
    ClassicProcessingLease, PreparedClassicProcessingLeaseActivation,
    PreparedClassicProcessingLeaseReconciliation, PreparedClassicProcessingLeaseRevocation,
};

/// Positive duration between application progress observations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClassicProcessingLeasePolicy {
    timeout_ticks: u64,
}

impl ClassicProcessingLeasePolicy {
    /// Validates a positive processing-liveness timeout.
    pub const fn try_new(timeout_ticks: u64) -> Result<Self, ClassicProcessingLeasePolicyError> {
        if timeout_ticks == 0 {
            return Err(ClassicProcessingLeasePolicyError::TimeoutZero);
        }
        Ok(Self { timeout_ticks })
    }

    /// Returns the positive lease duration in deterministic clock ticks.
    pub const fn timeout_ticks(self) -> u64 {
        self.timeout_ticks
    }
}

/// Invalid application-processing timing policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicProcessingLeasePolicyError {
    /// Progress must always move the deadline into the future.
    TimeoutZero,
}

impl core::fmt::Display for ClassicProcessingLeasePolicyError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("classic processing lease timeout must be positive")
    }
}

impl std::error::Error for ClassicProcessingLeasePolicyError {}

/// Exact group assignment whose application liveness is being observed.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClassicProcessingLeaseFence {
    group_id: GroupId,
    cycle: MembershipCycle,
    assignment_generation: AssignmentGeneration,
}

impl ClassicProcessingLeaseFence {
    /// Creates one fence from already-authoritative membership identities.
    pub const fn new(
        group_id: GroupId,
        cycle: MembershipCycle,
        assignment_generation: AssignmentGeneration,
    ) -> Self {
        Self {
            group_id,
            cycle,
            assignment_generation,
        }
    }

    /// Returns the stable group identity.
    pub const fn group_id(self) -> GroupId {
        self.group_id
    }

    /// Returns the nonreused membership cycle.
    pub const fn cycle(self) -> MembershipCycle {
        self.cycle
    }

    /// Returns the core-owned assignment generation.
    pub const fn assignment_generation(self) -> AssignmentGeneration {
        self.assignment_generation
    }
}

/// One active application-processing deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClassicProcessingLeaseSchedule {
    fence: ClassicProcessingLeaseFence,
    deadline: Deadline,
}

impl ClassicProcessingLeaseSchedule {
    const fn new(fence: ClassicProcessingLeaseFence, deadline: Deadline) -> Self {
        Self { fence, deadline }
    }

    /// Returns the assignment fence that owns this deadline.
    pub const fn fence(self) -> ClassicProcessingLeaseFence {
        self.fence
    }

    /// Returns the exact absolute processing deadline.
    pub const fn deadline(self) -> Deadline {
        self.deadline
    }
}

/// Why application liveness no longer protects one assignment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicProcessingLeaseExpirationReason {
    /// The active absolute deadline was observed at or after its boundary.
    DeadlineElapsed,
    /// A progress observation could not form another absolute deadline.
    DeadlineOverflow,
}

/// Retained assignment-loss fact awaiting membership revocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClassicProcessingLeaseExpiration {
    schedule: ClassicProcessingLeaseSchedule,
    reason: ClassicProcessingLeaseExpirationReason,
}

impl ClassicProcessingLeaseExpiration {
    /// Returns the exact expired schedule.
    pub const fn schedule(self) -> ClassicProcessingLeaseSchedule {
        self.schedule
    }

    /// Returns the deterministic expiration reason.
    pub const fn reason(self) -> ClassicProcessingLeaseExpirationReason {
        self.reason
    }
}

/// Explicit application-liveness fact supplied by the engine interpreter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicProcessingLeaseInput {
    /// Starts liveness when one exact assignment becomes externally usable.
    Activate {
        /// Exact installed group assignment.
        fence: ClassicProcessingLeaseFence,
        /// Current monotonic observation.
        now: Moment,
    },
    /// Renews liveness after application receipt, acknowledgement, or progress.
    Progress {
        /// Exact assignment observed by the application.
        fence: ClassicProcessingLeaseFence,
        /// Current monotonic observation.
        now: Moment,
    },
    /// Proves that one retained processing deadline is due.
    DeadlineElapsed {
        /// Exact assignment owning the scheduled deadline.
        fence: ClassicProcessingLeaseFence,
        /// Current monotonic observation.
        now: Moment,
    },
    /// Releases processing ownership after exact assignment revocation.
    AssignmentRevoked {
        /// Exact assignment removed by membership policy.
        fence: ClassicProcessingLeaseFence,
    },
}

/// Mechanism instruction emitted by processing-liveness policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicProcessingLeaseEffect {
    /// Schedule or replace one exact application-processing deadline.
    Arm {
        /// Exact assignment-fenced deadline.
        schedule: ClassicProcessingLeaseSchedule,
    },
    /// Revoke one assignment because application progress stopped.
    AssignmentLost {
        /// Exact retained expiration fact.
        expiration: ClassicProcessingLeaseExpiration,
    },
}

/// Bounded ordered effects from one processing-lease transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClassicProcessingLeaseTransition {
    effect: Option<ClassicProcessingLeaseEffect>,
}

impl ClassicProcessingLeaseTransition {
    const fn none() -> Self {
        Self { effect: None }
    }

    const fn one(effect: ClassicProcessingLeaseEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Returns zero or one ordered mechanism instruction.
    pub fn effects(&self) -> impl ExactSizeIterator<Item = &ClassicProcessingLeaseEffect> {
        self.effect.iter()
    }
}

/// Rejected liveness fact that leaves the exact prior state unchanged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicProcessingLeaseError {
    /// Activation was attempted while another assignment remained retained.
    AlreadyActive,
    /// Progress or revocation named no active assignment.
    NotActive,
    /// A fact named an assignment other than the retained owner.
    FenceMismatch,
    /// The retained deadline had not elapsed at the supplied observation.
    DeadlineNotElapsed,
    /// Initial activation could not form its absolute deadline.
    DeadlineOverflow,
    /// Assignment loss was already retained and awaits exact revocation.
    ExpirationPending,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum ClassicProcessingLeaseState {
    Dormant,
    Armed(ClassicProcessingLeaseSchedule),
    Expired(ClassicProcessingLeaseExpiration),
}
