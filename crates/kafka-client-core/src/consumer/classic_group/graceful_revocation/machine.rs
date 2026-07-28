//! Deterministic transitions for one assignment-fenced revocation lease.

use crate::{Deadline, Moment};

use super::{
    ClassicGracefulRevocationEffect, ClassicGracefulRevocationError,
    ClassicGracefulRevocationInput, ClassicGracefulRevocationLease,
    ClassicGracefulRevocationLossReason, ClassicGracefulRevocationTerminal,
    ClassicGracefulRevocationTransition, model::ClassicGracefulRevocationState,
};
use crate::consumer::AssignmentEpoch;

/// Deterministic owner of at most one graceful-revocation lease.
#[derive(Debug, Eq, PartialEq)]
pub struct ClassicGracefulRevocation {
    state: ClassicGracefulRevocationState,
}

impl ClassicGracefulRevocation {
    /// Creates one dormant owner without consulting time.
    pub const fn new() -> Self {
        Self {
            state: ClassicGracefulRevocationState::Dormant,
        }
    }

    /// Applies one exact revocation fact.
    pub fn apply(
        &mut self,
        input: ClassicGracefulRevocationInput,
    ) -> Result<ClassicGracefulRevocationTransition, ClassicGracefulRevocationError> {
        match input {
            ClassicGracefulRevocationInput::Begin { lease, now } => self.begin(lease, now),
            ClassicGracefulRevocationInput::Acknowledge {
                assignment_epoch,
                now,
            } => self.acknowledge(assignment_epoch, now),
            ClassicGracefulRevocationInput::DeadlineElapsed {
                assignment_epoch,
                now,
            } => self.deadline_elapsed(assignment_epoch, now),
            ClassicGracefulRevocationInput::OwnerLost { assignment_epoch } => {
                self.owner_lost(assignment_epoch)
            }
            ClassicGracefulRevocationInput::Release { assignment_epoch } => {
                self.release(assignment_epoch)
            }
        }
    }

    /// Returns the sole deadline that must participate in scheduling.
    pub const fn next_deadline(&self) -> Option<Deadline> {
        match self.state {
            ClassicGracefulRevocationState::Awaiting(lease) => Some(lease.deadline()),
            ClassicGracefulRevocationState::Dormant
            | ClassicGracefulRevocationState::Terminal(_) => None,
        }
    }

    /// Returns the exact lease while acknowledgment is still possible.
    pub const fn active_lease(&self) -> Option<ClassicGracefulRevocationLease> {
        match self.state {
            ClassicGracefulRevocationState::Awaiting(lease) => Some(lease),
            ClassicGracefulRevocationState::Dormant
            | ClassicGracefulRevocationState::Terminal(_) => None,
        }
    }

    /// Returns the exact retained terminal, if settlement has finished.
    pub const fn terminal(&self) -> Option<ClassicGracefulRevocationTerminal> {
        match self.state {
            ClassicGracefulRevocationState::Terminal(terminal) => Some(terminal),
            ClassicGracefulRevocationState::Dormant
            | ClassicGracefulRevocationState::Awaiting(_) => None,
        }
    }

    fn begin(
        &mut self,
        lease: ClassicGracefulRevocationLease,
        now: Moment,
    ) -> Result<ClassicGracefulRevocationTransition, ClassicGracefulRevocationError> {
        match self.state {
            ClassicGracefulRevocationState::Awaiting(_) => {
                return Err(ClassicGracefulRevocationError::AlreadyActive);
            }
            ClassicGracefulRevocationState::Terminal(_) => {
                return Err(ClassicGracefulRevocationError::TerminalRetained);
            }
            ClassicGracefulRevocationState::Dormant => {}
        }
        if lease.deadline().is_elapsed_at(now) {
            return Ok(self.lose(lease, ClassicGracefulRevocationLossReason::DeadlineElapsed));
        }
        self.state = ClassicGracefulRevocationState::Awaiting(lease);
        Ok(ClassicGracefulRevocationTransition::one(
            ClassicGracefulRevocationEffect::Arm { lease },
        ))
    }

    fn acknowledge(
        &mut self,
        assignment_epoch: AssignmentEpoch,
        now: Moment,
    ) -> Result<ClassicGracefulRevocationTransition, ClassicGracefulRevocationError> {
        let lease = self.require_awaiting(assignment_epoch)?;
        if lease.deadline().is_elapsed_at(now) {
            return Ok(self.lose(lease, ClassicGracefulRevocationLossReason::DeadlineElapsed));
        }
        Ok(self.finish(ClassicGracefulRevocationTerminal::Acknowledged(lease)))
    }

    fn deadline_elapsed(
        &mut self,
        assignment_epoch: AssignmentEpoch,
        now: Moment,
    ) -> Result<ClassicGracefulRevocationTransition, ClassicGracefulRevocationError> {
        let lease = self.require_awaiting(assignment_epoch)?;
        if !lease.deadline().is_elapsed_at(now) {
            return Err(ClassicGracefulRevocationError::DeadlineNotElapsed);
        }
        Ok(self.lose(lease, ClassicGracefulRevocationLossReason::DeadlineElapsed))
    }

    fn owner_lost(
        &mut self,
        assignment_epoch: AssignmentEpoch,
    ) -> Result<ClassicGracefulRevocationTransition, ClassicGracefulRevocationError> {
        let lease = self.require_awaiting(assignment_epoch)?;
        Ok(self.lose(lease, ClassicGracefulRevocationLossReason::OwnerLost))
    }

    fn release(
        &mut self,
        assignment_epoch: AssignmentEpoch,
    ) -> Result<ClassicGracefulRevocationTransition, ClassicGracefulRevocationError> {
        match self.state {
            ClassicGracefulRevocationState::Dormant => {
                Err(ClassicGracefulRevocationError::NotActive)
            }
            ClassicGracefulRevocationState::Awaiting(lease)
                if lease.assignment_epoch() != assignment_epoch =>
            {
                Err(ClassicGracefulRevocationError::AssignmentEpochMismatch)
            }
            ClassicGracefulRevocationState::Awaiting(_) => {
                Err(ClassicGracefulRevocationError::NotTerminal)
            }
            ClassicGracefulRevocationState::Terminal(terminal)
                if terminal.lease().assignment_epoch() != assignment_epoch =>
            {
                Err(ClassicGracefulRevocationError::AssignmentEpochMismatch)
            }
            ClassicGracefulRevocationState::Terminal(_) => {
                self.state = ClassicGracefulRevocationState::Dormant;
                Ok(ClassicGracefulRevocationTransition::none())
            }
        }
    }

    fn require_awaiting(
        &self,
        assignment_epoch: AssignmentEpoch,
    ) -> Result<ClassicGracefulRevocationLease, ClassicGracefulRevocationError> {
        match self.state {
            ClassicGracefulRevocationState::Dormant => {
                Err(ClassicGracefulRevocationError::NotActive)
            }
            ClassicGracefulRevocationState::Awaiting(lease)
                if lease.assignment_epoch() != assignment_epoch =>
            {
                Err(ClassicGracefulRevocationError::AssignmentEpochMismatch)
            }
            ClassicGracefulRevocationState::Awaiting(lease) => Ok(lease),
            ClassicGracefulRevocationState::Terminal(terminal)
                if terminal.lease().assignment_epoch() != assignment_epoch =>
            {
                Err(ClassicGracefulRevocationError::AssignmentEpochMismatch)
            }
            ClassicGracefulRevocationState::Terminal(_) => {
                Err(ClassicGracefulRevocationError::TerminalRetained)
            }
        }
    }

    fn lose(
        &mut self,
        lease: ClassicGracefulRevocationLease,
        reason: ClassicGracefulRevocationLossReason,
    ) -> ClassicGracefulRevocationTransition {
        self.finish(ClassicGracefulRevocationTerminal::Lost { lease, reason })
    }

    fn finish(
        &mut self,
        terminal: ClassicGracefulRevocationTerminal,
    ) -> ClassicGracefulRevocationTransition {
        self.state = ClassicGracefulRevocationState::Terminal(terminal);
        ClassicGracefulRevocationTransition::one(ClassicGracefulRevocationEffect::Complete {
            terminal,
        })
    }
}

impl Default for ClassicGracefulRevocation {
    fn default() -> Self {
        Self::new()
    }
}
