//! Narrow position access, fencing helpers, and test-only epoch forwarding.

use super::AssignedPartitionState;
use crate::{
    Deadline, Moment,
    consumer::{
        AssignedConsumerEffect, AssignedConsumerMachineError, AssignmentEpoch, PositionFence,
        position_state::{PartitionPosition, RetainedAssignmentPositionPlan},
    },
};

impl AssignedPartitionState {
    #[cfg(test)]
    pub(in crate::consumer) fn replace_position_epoch_for_test(
        &mut self,
        epoch: crate::consumer::PositionEpoch,
    ) {
        self.position.replace_epoch_for_test(epoch);
    }

    #[cfg(test)]
    pub(in crate::consumer) fn replace_fetch_revision_for_test(
        &mut self,
        revision: crate::consumer::FetchRevision,
    ) {
        self.position.replace_next_fetch_revision_for_test(revision);
    }

    pub(in crate::consumer) const fn is_paused(&self) -> bool {
        self.paused
    }

    pub(in crate::consumer) const fn position_state(&self) -> &PartitionPosition {
        &self.position
    }

    pub(in crate::consumer) const fn position_fence(
        &self,
        assignment_epoch: AssignmentEpoch,
    ) -> PositionFence {
        PositionFence::new(assignment_epoch, self.partition, self.position.epoch())
    }

    pub(in crate::consumer) fn plan_assignment_reconciliation(
        &self,
        new_assignment_epoch: AssignmentEpoch,
        now: Moment,
    ) -> Result<RetainedAssignmentPositionPlan, AssignedConsumerMachineError> {
        self.position.plan_assignment_reconciliation(
            new_assignment_epoch,
            self.partition,
            self.paused,
            now,
        )
    }

    pub(in crate::consumer) fn install_assignment_reconciliation(
        &mut self,
        plan: RetainedAssignmentPositionPlan,
    ) -> Option<AssignedConsumerEffect> {
        self.position.install_assignment_reconciliation(plan)
    }

    pub(super) fn activate(
        &mut self,
        assignment_epoch: AssignmentEpoch,
        now: Moment,
        deadline: Deadline,
    ) -> Result<Option<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        let fence = self.position_fence(assignment_epoch);
        self.position.activate(fence, self.partition, now, deadline)
    }

    pub(super) fn fence_position(&mut self) -> Result<(), AssignedConsumerMachineError> {
        self.position.fence(self.partition)
    }

    pub(super) fn ensure_position_fence(
        &self,
        supplied: PositionFence,
    ) -> Result<(), AssignedConsumerMachineError> {
        let active = self.position_fence(supplied.assignment_epoch());
        if active == supplied {
            Ok(())
        } else {
            Err(AssignedConsumerMachineError::StalePosition { active, supplied })
        }
    }
}
