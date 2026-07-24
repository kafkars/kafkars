//! Directional core policy for already-authorized retained Fetch deliveries.

use super::{
    AssignedConsumerMachine, AssignedConsumerMachineError, DeliveryOwnership, FetchFence,
    position::AssignedPartitionState,
};

impl AssignedPartitionState {
    fn delivery_ownership(
        &self,
        fence: FetchFence,
    ) -> Result<DeliveryOwnership, AssignedConsumerMachineError> {
        let supplied = fence.position();
        let active = self.position_fence(supplied.assignment_epoch());
        if supplied.position_epoch() < active.position_epoch() {
            return Ok(DeliveryOwnership::Superseded);
        }
        if supplied.position_epoch() > active.position_epoch() {
            return Err(AssignedConsumerMachineError::StalePosition { active, supplied });
        }
        Ok(DeliveryOwnership::Active)
    }
}

impl AssignedConsumerMachine {
    /// Classifies retained delivery ownership without reviving settled Fetch revisions.
    pub fn delivery_ownership(
        &self,
        fence: FetchFence,
    ) -> Result<DeliveryOwnership, AssignedConsumerMachineError> {
        let assignment = self
            .assignment
            .as_ref()
            .ok_or(AssignedConsumerMachineError::NoAssignment)?;
        if fence.position().assignment_epoch() < assignment.epoch {
            return Ok(DeliveryOwnership::Superseded);
        }
        if fence.position().assignment_epoch() > assignment.epoch {
            return Err(AssignedConsumerMachineError::StaleAssignment {
                active: assignment.epoch,
                supplied: fence.position().assignment_epoch(),
            });
        }
        assignment
            .partitions
            .iter()
            .find(|state| state.partition == fence.position().partition())
            .ok_or(AssignedConsumerMachineError::UnknownPartition {
                partition: fence.position().partition(),
            })?
            .delivery_ownership(fence)
    }
}
