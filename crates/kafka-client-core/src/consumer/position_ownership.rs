//! Machine-level directional ownership query for queued position resolution.

use super::{
    AssignedConsumerMachine, AssignedConsumerMachineError, PositionFence, PositionOwnership,
};

impl AssignedConsumerMachine {
    /// Reports whether one prepared lookup still owns the active resolution.
    ///
    /// Interpreters use this before acquiring bounded driver-call capacity for
    /// work that may have waited outside the deterministic machine.
    pub fn position_ownership(
        &self,
        fence: PositionFence,
    ) -> Result<PositionOwnership, AssignedConsumerMachineError> {
        let assignment = self
            .assignment
            .as_ref()
            .ok_or(AssignedConsumerMachineError::NoAssignment)?;
        if fence.assignment_epoch() < assignment.epoch {
            return Ok(PositionOwnership::Superseded);
        }
        if fence.assignment_epoch() > assignment.epoch {
            return Err(AssignedConsumerMachineError::StaleAssignment {
                active: assignment.epoch,
                supplied: fence.assignment_epoch(),
            });
        }
        assignment
            .partitions
            .iter()
            .find(|state| state.partition == fence.partition())
            .ok_or(AssignedConsumerMachineError::UnknownPartition {
                partition: fence.partition(),
            })?
            .position_ownership(fence)
    }
}
