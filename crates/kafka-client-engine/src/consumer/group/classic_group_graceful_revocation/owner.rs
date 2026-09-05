//! Pre-reserved per-entry ownership of completion and one retained core terminal.

use kafka_client_core::{
    AssignmentEpoch, ClassicGracefulRevocation, ClassicGracefulRevocationEffect,
    ClassicGracefulRevocationInput, ClassicGracefulRevocationLossReason,
    ClassicGracefulRevocationTerminal, LiveGroupAssignment, Moment,
};

use super::model::{
    ClassicGroupRevocationAcknowledgeError, ClassicGroupRevocationHostError,
    PendingGroupRevocation, one_effect,
};

/// One fixed-capacity owner constructed before any revocation is admitted.
pub(in crate::consumer::group) struct ClassicGroupRevocationOwner {
    pub(super) core: ClassicGracefulRevocation,
    pub(super) pending: Option<PendingGroupRevocation>,
}

impl ClassicGroupRevocationOwner {
    pub(in crate::consumer::group) const fn new() -> Self {
        Self {
            core: ClassicGracefulRevocation::new(),
            pending: None,
        }
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

    pub(in crate::consumer::group) fn acknowledge_public(
        &mut self,
        public_epoch: u64,
        now: Moment,
    ) -> Result<(), ClassicGroupRevocationAcknowledgeError> {
        let lease = self
            .core
            .active_lease()
            .ok_or(ClassicGroupRevocationAcknowledgeError::NoActiveLease)?;
        let assignment = match &self.pending {
            Some(
                PendingGroupRevocation::Classic(pending)
                | PendingGroupRevocation::ClassicReconciliation(pending),
            ) => &pending.assignment,
            Some(PendingGroupRevocation::Consumer(assignment)) => assignment,
            None => return Err(ClassicGroupRevocationAcknowledgeError::UnexpectedEffect),
        };
        if assignment.assignment_generation().get() != public_epoch {
            return Err(ClassicGroupRevocationAcknowledgeError::AssignmentEpochMismatch);
        }
        self.acknowledge(lease.assignment_epoch(), now)
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

    pub(in crate::consumer::group) const fn pending_is_consumer(&self) -> bool {
        matches!(&self.pending, Some(PendingGroupRevocation::Consumer(_)))
    }

    pub(in crate::consumer::group) const fn pending_is_classic_reconciliation(&self) -> bool {
        matches!(
            &self.pending,
            Some(PendingGroupRevocation::ClassicReconciliation(_))
        )
    }

    pub(super) fn take_pending(&mut self) -> Option<PendingGroupRevocation> {
        self.pending.take()
    }

    pub(super) fn restore_pending(&mut self, pending: PendingGroupRevocation) {
        self.pending = Some(pending);
    }

    pub(in crate::consumer::group) fn take_pending_consumer(
        &mut self,
    ) -> Option<LiveGroupAssignment> {
        let pending = self.pending.take()?;
        match pending {
            PendingGroupRevocation::Consumer(assignment) => Some(assignment),
            pending @ (PendingGroupRevocation::Classic(_)
            | PendingGroupRevocation::ClassicReconciliation(_)) => {
                self.pending = Some(pending);
                None
            }
        }
    }

    pub(in crate::consumer::group) fn take_pending_classic_reconciliation(
        &mut self,
    ) -> Option<(LiveGroupAssignment, kafka_client_core::ClassicGeneration)> {
        let pending = self.pending.take()?;
        match pending {
            PendingGroupRevocation::ClassicReconciliation(pending) => {
                Some((pending.assignment, pending.generation))
            }
            pending => {
                self.pending = Some(pending);
                None
            }
        }
    }

    pub(in crate::consumer::group) fn restore_pending_classic_reconciliation(
        &mut self,
        assignment: LiveGroupAssignment,
        generation: kafka_client_core::ClassicGeneration,
    ) {
        self.pending = Some(PendingGroupRevocation::classic_reconciliation(
            assignment, generation,
        ));
    }

    pub(in crate::consumer::group) fn restore_pending_consumer(
        &mut self,
        assignment: LiveGroupAssignment,
    ) {
        self.pending = Some(PendingGroupRevocation::consumer(assignment));
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
