//! Sole mutation owner for one assigned partition's pause and fetch position.

use super::{
    AssignedConsumerEffect, AssignedConsumerMachineError, AssignedPartition,
    AssignedTopicPartition, AssignmentEpoch, FetchFence, NextFetchOffset, PositionFence,
    StartPosition, position_state::PartitionPosition,
};
use crate::{Deadline, Moment};

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
        now: Moment,
        deadline: Deadline,
    ) -> Result<(Self, AssignedConsumerEffect), AssignedConsumerMachineError> {
        let mut state = Self {
            partition: assigned.partition(),
            paused: false,
            position: PartitionPosition::new(assigned.start()),
        };
        let effect = state.activate(assignment_epoch, now, deadline)?.ok_or(
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
        now: Moment,
        deadline: Deadline,
    ) -> Result<Option<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        if !self.paused {
            return Ok(None);
        }
        self.paused = false;
        self.activate(assignment_epoch, now, deadline)
    }

    pub(super) fn seek(
        &mut self,
        assignment_epoch: AssignmentEpoch,
        position: StartPosition,
        now: Moment,
        deadline: Deadline,
    ) -> Result<Vec<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        self.fence_position()?;
        self.position.replace(position);
        let mut effects = vec![AssignedConsumerEffect::Suspend {
            fence: self.position_fence(assignment_epoch),
        }];
        if !self.paused {
            if let Some(effect) = self.activate(assignment_epoch, now, deadline)? {
                effects.push(effect);
            }
        }
        Ok(effects)
    }

    pub(super) fn position_resolved(
        &mut self,
        fence: PositionFence,
        next_offset: NextFetchOffset,
        now: Moment,
        throttle_ticks: u64,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        self.ensure_position_fence(fence)?;
        self.position
            .resolve(fence, next_offset, now, throttle_ticks, self.partition)
    }

    pub(super) fn position_resolution_failed(
        &mut self,
        fence: PositionFence,
        now: Moment,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        self.ensure_position_fence(fence)?;
        self.position.fail(fence, now)
    }

    pub(super) fn position_resolution_deadline_elapsed(
        &mut self,
        fence: PositionFence,
        now: Moment,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        self.ensure_position_fence(fence)?;
        self.position.resolution_deadline_elapsed(fence, now)
    }

    pub(super) fn position_throttle_elapsed(
        &mut self,
        fence: PositionFence,
        now: Moment,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        self.ensure_position_fence(fence)?;
        self.position.throttle_elapsed(fence, now, self.partition)
    }

    pub(super) fn fetch_advanced(
        &mut self,
        supplied: FetchFence,
        next_offset: NextFetchOffset,
    ) -> Result<Option<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        self.ensure_position_fence(supplied.position())?;
        if self.paused {
            return Err(AssignedConsumerMachineError::StaleFetch { supplied });
        }
        self.position
            .advance_and_activate(supplied, next_offset, self.partition)
            .map(Some)
    }

    fn activate(
        &mut self,
        assignment_epoch: AssignmentEpoch,
        now: Moment,
        deadline: Deadline,
    ) -> Result<Option<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        let fence = self.position_fence(assignment_epoch);
        self.position.activate(fence, self.partition, now, deadline)
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
