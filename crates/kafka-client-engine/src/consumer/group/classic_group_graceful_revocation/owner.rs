//! Pre-reserved per-entry ownership of completion and one retained core terminal.

use kafka_client_core::{
    AssignmentEpoch, ClassicGeneration, ClassicGracefulRevocation, ClassicGracefulRevocationEffect,
    ClassicGracefulRevocationInput, ClassicGracefulRevocationLease,
    ClassicGracefulRevocationLossReason, ClassicGracefulRevocationTerminal, LiveGroupAssignment,
    Moment,
};

use super::model::{
    ClassicGroupRevocationAcknowledgeError, ClassicGroupRevocationBeginError,
    ClassicGroupRevocationHostError, PendingClassicGroupRevocation,
};

/// One fixed-capacity owner constructed before any revocation is admitted.
pub(in crate::consumer::group) struct ClassicGroupRevocationOwner {
    core: ClassicGracefulRevocation,
    pending: Option<PendingClassicGroupRevocation>,
}

impl ClassicGroupRevocationOwner {
    pub(in crate::consumer::group) const fn new() -> Self {
        Self {
            core: ClassicGracefulRevocation::new(),
            pending: None,
        }
    }

    pub(in crate::consumer::group) fn begin(
        &mut self,
        assignment: LiveGroupAssignment,
        generation: ClassicGeneration,
        lease: ClassicGracefulRevocationLease,
        now: Moment,
    ) -> Result<(), (ClassicGroupRevocationBeginError, LiveGroupAssignment)> {
        if self.pending.is_some()
            || self.core.active_lease().is_some()
            || self.core.terminal().is_some()
        {
            return Err((ClassicGroupRevocationBeginError::Occupied, assignment));
        }
        let transition = match self
            .core
            .apply(ClassicGracefulRevocationInput::Begin { lease, now })
        {
            Ok(transition) => transition,
            Err(error) => {
                return Err((ClassicGroupRevocationBeginError::Core(error), assignment));
            }
        };
        let effect = one_effect(&transition);
        match effect {
            Some(ClassicGracefulRevocationEffect::Arm { lease: armed }) if armed == lease => {}
            Some(ClassicGracefulRevocationEffect::Complete {
                terminal:
                    ClassicGracefulRevocationTerminal::Lost {
                        lease: expired,
                        reason: ClassicGracefulRevocationLossReason::DeadlineElapsed,
                    },
            }) if expired == lease => {}
            _ => {
                return Err((
                    ClassicGroupRevocationBeginError::UnexpectedEffect,
                    assignment,
                ));
            }
        }
        self.pending = Some(PendingClassicGroupRevocation::new(assignment, generation));
        Ok(())
    }

    pub(in crate::consumer::group) fn acknowledge(
        &mut self,
        assignment_epoch: AssignmentEpoch,
        now: Moment,
    ) -> Result<(), ClassicGroupRevocationAcknowledgeError> {
        let transition = self
            .core
            .apply(ClassicGracefulRevocationInput::Acknowledge {
                assignment_epoch,
                now,
            })
            .map_err(ClassicGroupRevocationAcknowledgeError::Core)?;
        let terminal = self
            .core
            .terminal()
            .ok_or(ClassicGroupRevocationAcknowledgeError::UnexpectedEffect)?;
        if one_effect(&transition) != Some(ClassicGracefulRevocationEffect::Complete { terminal }) {
            return Err(ClassicGroupRevocationAcknowledgeError::UnexpectedEffect);
        }
        match terminal {
            ClassicGracefulRevocationTerminal::Acknowledged(lease)
                if lease.assignment_epoch() == assignment_epoch =>
            {
                Ok(())
            }
            ClassicGracefulRevocationTerminal::Lost {
                lease,
                reason: ClassicGracefulRevocationLossReason::DeadlineElapsed,
            } if lease.assignment_epoch() == assignment_epoch => {
                Err(ClassicGroupRevocationAcknowledgeError::DeadlineElapsed)
            }
            _ => Err(ClassicGroupRevocationAcknowledgeError::UnexpectedEffect),
        }
    }

    pub(in crate::consumer::group) const fn active_assignment_epoch(
        &self,
    ) -> Option<AssignmentEpoch> {
        match self.core.active_lease() {
            Some(lease) => Some(lease.assignment_epoch()),
            None => None,
        }
    }

    pub(in crate::consumer::group) fn expire_if_due(
        &mut self,
        now: Moment,
    ) -> Result<bool, ClassicGroupRevocationHostError> {
        let Some(lease) = self.core.active_lease() else {
            return Ok(false);
        };
        if !lease.deadline().is_elapsed_at(now) {
            return Ok(false);
        }
        let transition = self
            .core
            .apply(ClassicGracefulRevocationInput::DeadlineElapsed {
                assignment_epoch: lease.assignment_epoch(),
                now,
            })
            .map_err(ClassicGroupRevocationHostError::Core)?;
        let terminal = self
            .core
            .terminal()
            .ok_or(ClassicGroupRevocationHostError::UnexpectedEffect)?;
        if one_effect(&transition) != Some(ClassicGracefulRevocationEffect::Complete { terminal }) {
            return Err(ClassicGroupRevocationHostError::UnexpectedEffect);
        }
        Ok(true)
    }

    pub(in crate::consumer::group) fn lose_owner(
        &mut self,
    ) -> Result<bool, ClassicGroupRevocationHostError> {
        let Some(lease) = self.core.active_lease() else {
            return Ok(false);
        };
        let transition = self
            .core
            .apply(ClassicGracefulRevocationInput::OwnerLost {
                assignment_epoch: lease.assignment_epoch(),
            })
            .map_err(ClassicGroupRevocationHostError::Core)?;
        let terminal = self
            .core
            .terminal()
            .ok_or(ClassicGroupRevocationHostError::UnexpectedEffect)?;
        if one_effect(&transition) != Some(ClassicGracefulRevocationEffect::Complete { terminal }) {
            return Err(ClassicGroupRevocationHostError::UnexpectedEffect);
        }
        Ok(true)
    }

    pub(in crate::consumer::group) const fn next_deadline(
        &self,
    ) -> Option<kafka_client_core::Deadline> {
        self.core.next_deadline()
    }

    pub(in crate::consumer::group) const fn is_dormant(&self) -> bool {
        self.pending.is_none()
            && self.core.active_lease().is_none()
            && self.core.terminal().is_none()
    }

    pub(in crate::consumer::group) const fn terminal(
        &self,
    ) -> Option<ClassicGracefulRevocationTerminal> {
        self.core.terminal()
    }

    pub(super) fn take_pending(&mut self) -> Option<PendingClassicGroupRevocation> {
        self.pending.take()
    }

    pub(super) fn restore_pending(&mut self, pending: PendingClassicGroupRevocation) {
        self.pending = Some(pending);
    }

    pub(in crate::consumer::group) fn release_terminal(
        &mut self,
        assignment_epoch: AssignmentEpoch,
    ) -> Result<(), ClassicGroupRevocationHostError> {
        let transition = self
            .core
            .apply(ClassicGracefulRevocationInput::Release { assignment_epoch })
            .map_err(ClassicGroupRevocationHostError::Core)?;
        if transition.effects().next().is_some() {
            return Err(ClassicGroupRevocationHostError::UnexpectedEffect);
        }
        Ok(())
    }
}

fn one_effect(
    transition: &kafka_client_core::ClassicGracefulRevocationTransition,
) -> Option<ClassicGracefulRevocationEffect> {
    let mut effects = transition.effects().copied();
    let first = effects.next();
    if effects.next().is_some() {
        return None;
    }
    first
}
