//! Sole mutation owner for one assigned partition's pause and fetch position.

use super::{
    AssignedConsumerEffect, AssignedConsumerMachineError, AssignedPartition,
    AssignedTopicPartition, AssignmentEpoch, FetchFence, NextFetchOffset, PositionFence,
    StartPosition, position_state::PartitionPosition,
};

#[derive(Debug)]
pub(super) struct AssignedPartitionState {
    pub(super) partition: AssignedTopicPartition,
    paused: bool,
    position: PartitionPosition,
}

impl AssignedPartitionState {
    pub(super) fn new(
        assignment_epoch: AssignmentEpoch,
        assigned: AssignedPartition,
    ) -> Result<(Self, AssignedConsumerEffect), AssignedConsumerMachineError> {
        let mut state = Self {
            partition: assigned.partition(),
            paused: false,
            position: PartitionPosition::new(assigned.start()),
        };
        let effect = state.activate(assignment_epoch)?.ok_or(
            AssignedConsumerMachineError::PositionResolutionNotPending {
                fence: state.position_fence(assignment_epoch),
            },
        )?;
        Ok((state, effect))
    }

    pub(super) fn pause(
        &mut self,
        assignment_epoch: AssignmentEpoch,
    ) -> Result<Option<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        if self.paused {
            return Ok(None);
        }
        self.fence_position()?;
        self.paused = true;
        Ok(Some(AssignedConsumerEffect::Suspend {
            fence: self.position_fence(assignment_epoch),
        }))
    }

    pub(super) fn resume(
        &mut self,
        assignment_epoch: AssignmentEpoch,
    ) -> Result<Option<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        if !self.paused {
            return Ok(None);
        }
        self.paused = false;
        self.activate(assignment_epoch)
    }

    pub(super) fn seek(
        &mut self,
        assignment_epoch: AssignmentEpoch,
        position: StartPosition,
    ) -> Result<Vec<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        self.fence_position()?;
        self.position.replace(position);
        let mut effects = vec![AssignedConsumerEffect::Suspend {
            fence: self.position_fence(assignment_epoch),
        }];
        if !self.paused {
            if let Some(effect) = self.activate(assignment_epoch)? {
                effects.push(effect);
            }
        }
        Ok(effects)
    }

    pub(super) fn position_resolved(
        &mut self,
        fence: PositionFence,
        next_offset: NextFetchOffset,
    ) -> Result<Option<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        self.ensure_position_fence(fence)?;
        if !self.position.is_awaiting_resolution() {
            return Err(AssignedConsumerMachineError::PositionResolutionNotPending { fence });
        }
        self.position.resolve(next_offset);
        if self.paused {
            Ok(None)
        } else {
            self.activate(fence.assignment_epoch())
        }
    }

    pub(super) fn fetch_advanced(
        &mut self,
        supplied: FetchFence,
        next_offset: NextFetchOffset,
    ) -> Result<Option<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        self.ensure_position_fence(supplied.position())?;
        self.position.advance(supplied, next_offset)?;
        if self.paused {
            Ok(None)
        } else {
            self.activate(supplied.position().assignment_epoch())
        }
    }

    fn activate(
        &mut self,
        assignment_epoch: AssignmentEpoch,
    ) -> Result<Option<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        let fence = self.position_fence(assignment_epoch);
        self.position.activate(fence, self.partition)
    }

    fn fence_position(&mut self) -> Result<(), AssignedConsumerMachineError> {
        self.position.fence(self.partition)
    }

    fn ensure_position_fence(
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

    const fn position_fence(&self, assignment_epoch: AssignmentEpoch) -> PositionFence {
        PositionFence::new(assignment_epoch, self.partition, self.position.epoch())
    }
}
