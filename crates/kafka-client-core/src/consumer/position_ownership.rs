//! Directional ownership classification for queued position and Fetch work.

use super::{
    AssignedConsumerMachine, AssignedConsumerMachineError, FetchFence, FetchOwnership,
    PositionFence, PositionOwnership, position::AssignedPartitionState,
};

impl AssignedPartitionState {
    pub(super) fn position_ownership(
        &self,
        supplied: PositionFence,
    ) -> Result<PositionOwnership, AssignedConsumerMachineError> {
        let active = self.position_fence();
        if supplied.assignment_epoch() < active.assignment_epoch() {
            return Ok(PositionOwnership::Superseded);
        }
        if supplied.assignment_epoch() > active.assignment_epoch() {
            return Err(AssignedConsumerMachineError::StaleAssignment {
                active: active.assignment_epoch(),
                supplied: supplied.assignment_epoch(),
            });
        }
        if supplied.position_epoch() < active.position_epoch() {
            return Ok(PositionOwnership::Superseded);
        }
        if supplied.position_epoch() > active.position_epoch() {
            return Err(AssignedConsumerMachineError::StalePosition { active, supplied });
        }
        if self.is_paused() {
            return Err(AssignedConsumerMachineError::PositionResolutionNotPending {
                fence: supplied,
            });
        }
        self.position_state().position_ownership(supplied)
    }

    pub(super) fn fetch_ownership(
        &self,
        fence: FetchFence,
    ) -> Result<FetchOwnership, AssignedConsumerMachineError> {
        let supplied = fence.position();
        let active = self.position_fence();
        if supplied.assignment_epoch() < active.assignment_epoch() {
            return Ok(FetchOwnership::Superseded);
        }
        if supplied.assignment_epoch() > active.assignment_epoch() {
            return Err(AssignedConsumerMachineError::StaleAssignment {
                active: active.assignment_epoch(),
                supplied: supplied.assignment_epoch(),
            });
        }
        if supplied.position_epoch() < active.position_epoch() {
            return Ok(FetchOwnership::Superseded);
        }
        if supplied.position_epoch() > active.position_epoch() {
            return Err(AssignedConsumerMachineError::StalePosition { active, supplied });
        }
        if self.is_paused() {
            return Err(AssignedConsumerMachineError::StaleFetch { supplied: fence });
        }
        self.position_state().fetch_ownership(fence)
    }
}

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
        match assignment.find(fence.partition()) {
            Some(state) => state.position_ownership(fence),
            None if fence.assignment_epoch() < assignment.epoch => {
                Ok(PositionOwnership::Superseded)
            }
            None if fence.assignment_epoch() > assignment.epoch => {
                Err(AssignedConsumerMachineError::StaleAssignment {
                    active: assignment.epoch,
                    supplied: fence.assignment_epoch(),
                })
            }
            None => Err(AssignedConsumerMachineError::UnknownPartition {
                partition: fence.partition(),
            }),
        }
    }
}
