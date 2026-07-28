//! Deterministic transitions for one assignment-fenced processing lease.

use crate::{Deadline, Moment};

use super::{
    ClassicProcessingLeaseEffect, ClassicProcessingLeaseError, ClassicProcessingLeaseExpiration,
    ClassicProcessingLeaseExpirationReason, ClassicProcessingLeaseFence,
    ClassicProcessingLeaseInput, ClassicProcessingLeasePolicy, ClassicProcessingLeaseSchedule,
    ClassicProcessingLeaseState, ClassicProcessingLeaseTransition,
};

/// Deterministic owner of one classic member's application-processing lease.
#[derive(Debug, Eq, PartialEq)]
pub struct ClassicProcessingLease {
    policy: ClassicProcessingLeasePolicy,
    state: ClassicProcessingLeaseState,
}

/// Exclusive prepared activation that leaves a dormant owner unchanged on drop.
#[derive(Debug)]
#[must_use = "a prepared processing lease must be committed or explicitly dropped"]
pub struct PreparedClassicProcessingLeaseActivation<'lease> {
    owner: &'lease mut ClassicProcessingLease,
    schedule: ClassicProcessingLeaseSchedule,
}

/// Exclusive prepared release that preserves the exact lease on drop.
#[derive(Debug)]
#[must_use = "a prepared processing-lease revocation must be committed or dropped"]
pub struct PreparedClassicProcessingLeaseRevocation<'lease> {
    owner: &'lease mut ClassicProcessingLease,
    fence: ClassicProcessingLeaseFence,
}

impl ClassicProcessingLease {
    /// Creates one dormant lease without consulting time.
    pub const fn new(policy: ClassicProcessingLeasePolicy) -> Self {
        Self {
            policy,
            state: ClassicProcessingLeaseState::Dormant,
        }
    }

    /// Prepares one exact activation without mutating the dormant owner.
    pub fn prepare_activation(
        &mut self,
        fence: ClassicProcessingLeaseFence,
        now: Moment,
    ) -> Result<PreparedClassicProcessingLeaseActivation<'_>, ClassicProcessingLeaseError> {
        if !matches!(self.state, ClassicProcessingLeaseState::Dormant) {
            return Err(ClassicProcessingLeaseError::AlreadyActive);
        }
        let deadline = now
            .checked_deadline_after(self.policy.timeout_ticks())
            .ok_or(ClassicProcessingLeaseError::DeadlineOverflow)?;
        Ok(PreparedClassicProcessingLeaseActivation {
            owner: self,
            schedule: ClassicProcessingLeaseSchedule::new(fence, deadline),
        })
    }

    /// Prepares exact assignment revocation without changing lease ownership.
    pub fn prepare_revocation(
        &mut self,
        fence: ClassicProcessingLeaseFence,
    ) -> Result<PreparedClassicProcessingLeaseRevocation<'_>, ClassicProcessingLeaseError> {
        let retained = match self.state {
            ClassicProcessingLeaseState::Dormant => {
                return Err(ClassicProcessingLeaseError::NotActive);
            }
            ClassicProcessingLeaseState::Armed(schedule) => schedule.fence(),
            ClassicProcessingLeaseState::Expired(expiration) => expiration.schedule().fence(),
        };
        if retained != fence {
            return Err(ClassicProcessingLeaseError::FenceMismatch);
        }
        Ok(PreparedClassicProcessingLeaseRevocation { owner: self, fence })
    }

    /// Applies one explicit application-liveness fact.
    pub fn apply(
        &mut self,
        input: ClassicProcessingLeaseInput,
    ) -> Result<ClassicProcessingLeaseTransition, ClassicProcessingLeaseError> {
        match input {
            ClassicProcessingLeaseInput::Activate { fence, now } => self.activate(fence, now),
            ClassicProcessingLeaseInput::Progress { fence, now } => self.progress(fence, now),
            ClassicProcessingLeaseInput::DeadlineElapsed { fence, now } => {
                self.deadline_elapsed(fence, now)
            }
            ClassicProcessingLeaseInput::AssignmentRevoked { fence } => self.revoke(fence),
        }
    }

    /// Returns the exact deadline that must participate in host scheduling.
    pub const fn next_deadline(&self) -> Option<Deadline> {
        match self.state {
            ClassicProcessingLeaseState::Armed(schedule) => Some(schedule.deadline()),
            ClassicProcessingLeaseState::Dormant | ClassicProcessingLeaseState::Expired(_) => None,
        }
    }

    /// Returns the exact assignment-fenced active schedule, if armed.
    pub const fn active_schedule(&self) -> Option<ClassicProcessingLeaseSchedule> {
        match self.state {
            ClassicProcessingLeaseState::Armed(schedule) => Some(schedule),
            ClassicProcessingLeaseState::Dormant | ClassicProcessingLeaseState::Expired(_) => None,
        }
    }

    /// Returns an expiration retained until membership revokes the assignment.
    pub const fn pending_expiration(&self) -> Option<ClassicProcessingLeaseExpiration> {
        match self.state {
            ClassicProcessingLeaseState::Expired(expiration) => Some(expiration),
            ClassicProcessingLeaseState::Dormant | ClassicProcessingLeaseState::Armed(_) => None,
        }
    }

    fn activate(
        &mut self,
        fence: ClassicProcessingLeaseFence,
        now: Moment,
    ) -> Result<ClassicProcessingLeaseTransition, ClassicProcessingLeaseError> {
        Ok(self.prepare_activation(fence, now)?.commit())
    }

    fn progress(
        &mut self,
        fence: ClassicProcessingLeaseFence,
        now: Moment,
    ) -> Result<ClassicProcessingLeaseTransition, ClassicProcessingLeaseError> {
        let schedule = self.require_armed(fence)?;
        if schedule.deadline().is_elapsed_at(now) {
            return Ok(self.expire(
                schedule,
                ClassicProcessingLeaseExpirationReason::DeadlineElapsed,
            ));
        }
        let Some(deadline) = now.checked_deadline_after(self.policy.timeout_ticks()) else {
            return Ok(self.expire(
                schedule,
                ClassicProcessingLeaseExpirationReason::DeadlineOverflow,
            ));
        };
        let renewed = ClassicProcessingLeaseSchedule::new(fence, deadline);
        self.state = ClassicProcessingLeaseState::Armed(renewed);
        Ok(arm(renewed))
    }

    fn deadline_elapsed(
        &mut self,
        fence: ClassicProcessingLeaseFence,
        now: Moment,
    ) -> Result<ClassicProcessingLeaseTransition, ClassicProcessingLeaseError> {
        let schedule = self.require_armed(fence)?;
        if !schedule.deadline().is_elapsed_at(now) {
            return Err(ClassicProcessingLeaseError::DeadlineNotElapsed);
        }
        Ok(self.expire(
            schedule,
            ClassicProcessingLeaseExpirationReason::DeadlineElapsed,
        ))
    }

    fn revoke(
        &mut self,
        fence: ClassicProcessingLeaseFence,
    ) -> Result<ClassicProcessingLeaseTransition, ClassicProcessingLeaseError> {
        Ok(self.prepare_revocation(fence)?.commit())
    }

    fn require_armed(
        &self,
        fence: ClassicProcessingLeaseFence,
    ) -> Result<ClassicProcessingLeaseSchedule, ClassicProcessingLeaseError> {
        match self.state {
            ClassicProcessingLeaseState::Dormant => Err(ClassicProcessingLeaseError::NotActive),
            ClassicProcessingLeaseState::Expired(_) => {
                Err(ClassicProcessingLeaseError::ExpirationPending)
            }
            ClassicProcessingLeaseState::Armed(schedule) if schedule.fence() != fence => {
                Err(ClassicProcessingLeaseError::FenceMismatch)
            }
            ClassicProcessingLeaseState::Armed(schedule) => Ok(schedule),
        }
    }

    fn expire(
        &mut self,
        schedule: ClassicProcessingLeaseSchedule,
        reason: ClassicProcessingLeaseExpirationReason,
    ) -> ClassicProcessingLeaseTransition {
        let expiration = ClassicProcessingLeaseExpiration { schedule, reason };
        self.state = ClassicProcessingLeaseState::Expired(expiration);
        ClassicProcessingLeaseTransition::one(ClassicProcessingLeaseEffect::AssignmentLost {
            expiration,
        })
    }
}

impl PreparedClassicProcessingLeaseActivation<'_> {
    /// Returns the exact schedule that will become active on commit.
    pub const fn schedule(&self) -> ClassicProcessingLeaseSchedule {
        self.schedule
    }

    /// Atomically activates the already-validated schedule.
    pub fn commit(self) -> ClassicProcessingLeaseTransition {
        self.owner.state = ClassicProcessingLeaseState::Armed(self.schedule);
        arm(self.schedule)
    }
}

impl PreparedClassicProcessingLeaseRevocation<'_> {
    /// Returns the exact assignment whose lease will be released.
    pub const fn fence(&self) -> ClassicProcessingLeaseFence {
        self.fence
    }

    /// Releases the already-validated lease without emitting mechanism work.
    pub fn commit(self) -> ClassicProcessingLeaseTransition {
        self.owner.state = ClassicProcessingLeaseState::Dormant;
        ClassicProcessingLeaseTransition::none()
    }
}

const fn arm(schedule: ClassicProcessingLeaseSchedule) -> ClassicProcessingLeaseTransition {
    ClassicProcessingLeaseTransition::one(ClassicProcessingLeaseEffect::Arm { schedule })
}
