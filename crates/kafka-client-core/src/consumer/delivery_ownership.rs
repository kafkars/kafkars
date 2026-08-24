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
        let active = self.position_fence();
        if supplied.assignment_epoch() < active.assignment_epoch() {
            return Ok(DeliveryOwnership::Superseded);
        }
        if supplied.assignment_epoch() > active.assignment_epoch() {
            return Err(AssignedConsumerMachineError::StaleAssignment {
                active: active.assignment_epoch(),
                supplied: supplied.assignment_epoch(),
            });
        }
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
        match assignment.find(fence.position().partition()) {
            Some(state) => state.delivery_ownership(fence),
            None if fence.position().assignment_epoch() < assignment.epoch => {
                Ok(DeliveryOwnership::Superseded)
            }
            None if fence.position().assignment_epoch() > assignment.epoch => {
                Err(AssignedConsumerMachineError::StaleAssignment {
                    active: assignment.epoch,
                    supplied: fence.position().assignment_epoch(),
                })
            }
            None => Err(AssignedConsumerMachineError::UnknownPartition {
                partition: fence.position().partition(),
            }),
        }
    }
}
