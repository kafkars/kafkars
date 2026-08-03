//! Linear engine ownership between one cooperative Sync and assignment replacement.

use kafka_client_core::{
    ClassicAssignmentReconciliation, ClassicGeneration, ClassicGroupMachine, ClassicGroupPhase,
    ClassicRejoinSchedule, Deadline, LiveGroupAssignment,
};

use super::classic_group_position::ClassicGroupPositionPreparation;

/// Exact core replacement and preallocated position work retained across route confirmation.
#[must_use = "a prepared classic reconciliation must be applied or explicitly dropped"]
pub(super) struct PreparedClassicGroupReconciliation {
    reconciliation: ClassicAssignmentReconciliation,
    revocation_assignment: Option<LiveGroupAssignment>,
    position: Option<ClassicGroupPositionPreparation>,
    revocation_deadline: Deadline,
    sync_confirmed: bool,
    revocation_staged: bool,
    revocation_settled: bool,
    assignment_loss: Option<(LiveGroupAssignment, ClassicGeneration)>,
}

impl PreparedClassicGroupReconciliation {
    pub(super) fn new(
        reconciliation: ClassicAssignmentReconciliation,
        revocation_assignment: LiveGroupAssignment,
        position: ClassicGroupPositionPreparation,
        revocation_deadline: Deadline,
    ) -> Self {
        let revocation_settled = reconciliation.delta().removed().is_empty();
        Self {
            reconciliation,
            revocation_assignment: Some(revocation_assignment),
            position: Some(position),
            revocation_deadline,
            sync_confirmed: false,
            revocation_staged: false,
            revocation_settled,
            assignment_loss: None,
        }
    }

    pub(super) const fn reconciliation(&self) -> &ClassicAssignmentReconciliation {
        &self.reconciliation
    }

    pub(super) fn membership_ownership_matches(
        &self,
        machine: &ClassicGroupMachine,
        installed_rejoin: Option<ClassicRejoinSchedule>,
    ) -> bool {
        let replacement = self.reconciliation.replacement_assignment();
        let phase_matches = match machine.phase() {
            ClassicGroupPhase::Reconciling => {
                machine.pending_rejoin().is_none() && installed_rejoin.is_none()
            }
            ClassicGroupPhase::WaitingToRejoin => {
                machine.pending_rejoin().is_some_and(|schedule| {
                    installed_rejoin == Some(schedule)
                        && schedule.cycle() == self.reconciliation.replacement_cycle()
                        && schedule.assignment_generation()
                            == Some(replacement.assignment_generation())
                })
            }
            _ => false,
        };
        phase_matches
            && machine.live_assignment() == Some(replacement)
            && machine.live_cycle() == Some(self.reconciliation.replacement_cycle())
            && machine.live_generation()
                == Some(self.reconciliation.replacement_classic_generation())
    }

    pub(super) const fn revocation_deadline(&self) -> Deadline {
        self.revocation_deadline
    }

    pub(super) fn take_revocation_assignment(&mut self) -> Option<LiveGroupAssignment> {
        self.revocation_assignment.take()
    }

    pub(super) fn restore_revocation_assignment(&mut self, assignment: LiveGroupAssignment) {
        self.revocation_assignment = Some(assignment);
    }

    pub(super) const fn sync_is_confirmed(&self) -> bool {
        self.sync_confirmed
    }

    pub(super) fn confirm_sync(&mut self) {
        self.sync_confirmed = true;
    }

    pub(super) const fn revocation_is_staged(&self) -> bool {
        self.revocation_staged
    }

    pub(super) fn stage_revocation(&mut self) {
        self.revocation_staged = true;
    }

    pub(super) const fn revocation_is_settled(&self) -> bool {
        self.revocation_settled
    }

    pub(super) fn settle_revocation(&mut self) {
        self.revocation_settled = true;
    }

    pub(super) const fn assignment_loss_is_staged(&self) -> bool {
        self.assignment_loss.is_some()
    }

    pub(super) fn stage_assignment_loss(
        &mut self,
        assignment: LiveGroupAssignment,
        generation: ClassicGeneration,
    ) -> Result<(), (LiveGroupAssignment, ClassicGeneration)> {
        if self.assignment_loss.is_some()
            || self.reconciliation.replacement_assignment() != &assignment
            || self.reconciliation.replacement_classic_generation() != generation
        {
            return Err((assignment, generation));
        }
        self.assignment_loss = Some((assignment, generation));
        Ok(())
    }

    pub(super) fn assignment_loss(&self) -> Option<(&LiveGroupAssignment, ClassicGeneration)> {
        self.assignment_loss
            .as_ref()
            .map(|(assignment, generation)| (assignment, *generation))
    }

    pub(super) fn take_position(&mut self) -> Option<ClassicGroupPositionPreparation> {
        self.position.take()
    }

    pub(super) const fn position_was_installed(&self) -> bool {
        self.position.is_none()
    }

    pub(super) fn into_reconciliation(self) -> ClassicAssignmentReconciliation {
        self.reconciliation
    }
}
