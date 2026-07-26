//! Narrow read access to assigned partition positions, plus test-only epoch forwarding.

use super::AssignedPartitionState;
use crate::consumer::{AssignmentEpoch, PositionFence, position_state::PartitionPosition};

impl AssignedPartitionState {
    #[cfg(test)]
    pub(in crate::consumer) fn replace_position_epoch_for_test(
        &mut self,
        epoch: crate::consumer::PositionEpoch,
    ) {
        self.position.replace_epoch_for_test(epoch);
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
}
