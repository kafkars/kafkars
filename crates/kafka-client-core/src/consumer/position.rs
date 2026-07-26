//! Sole mutation owner for one assigned partition's pause and fetch position.

mod access;

use super::{
    AssignedConsumerEffect, AssignedConsumerMachineError, AssignedPartition,
    AssignedTopicPartition, AssignmentEpoch, FetchFailure, FetchFence, FetchRecords,
    NextFetchOffset, PositionEpoch, PositionFence, PositionResolutionAttemptFailure,
    ResolvedAssignedPartition, StartPosition, position_state::PartitionPosition,
};
use crate::{Deadline, Moment};

#[derive(Debug)]
pub(super) struct AssignedPartitionState {
    pub(super) partition: AssignedTopicPartition,
    paused: bool,
    position: PartitionPosition,
}

impl AssignedPartitionState {
    pub(super) fn plan_close(&self) -> Result<PositionEpoch, AssignedConsumerMachineError> {
        self.position.plan_fence(self.partition)
    }

    pub(super) fn suspend_for_close(
        &mut self,
        assignment_epoch: AssignmentEpoch,
        next_epoch: PositionEpoch,
    ) -> AssignedConsumerEffect {
        self.position.install_preflighted_fence(next_epoch);
        self.paused = true;
        AssignedConsumerEffect::Suspend {
            fence: self.position_fence(assignment_epoch),
        }
    }

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

    pub(super) fn new_resolved(
        assignment_epoch: AssignmentEpoch,
        assigned: ResolvedAssignedPartition,
        throttle_deadline: Option<Deadline>,
    ) -> (Self, AssignedConsumerEffect) {
        let mut state = Self {
            partition: assigned.partition(),
            paused: false,
            position: PartitionPosition::new(StartPosition::Offset(assigned.next_offset())),
        };
        let fence = state.position_fence(assignment_epoch);
        let effect = state.position.start_resolved_assignment_fetch(
            fence,
            assigned.next_offset(),
            throttle_deadline,
        );
        (state, effect)
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
        failure: PositionResolutionAttemptFailure,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        self.ensure_position_fence(fence)?;
        self.position.fail(fence, now, failure)
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
        records: FetchRecords,
        next_offset: NextFetchOffset,
        now: Moment,
        throttle_ticks: u64,
    ) -> Result<Vec<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        self.ensure_position_fence(supplied.position())?;
        if self.paused {
            return Err(AssignedConsumerMachineError::StaleFetch { supplied });
        }
        self.position.advance(
            supplied,
            records,
            next_offset,
            now,
            throttle_ticks,
            self.partition,
        )
    }

    pub(super) fn fetch_failed(
        &mut self,
        supplied: FetchFence,
        failure: FetchFailure,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        self.ensure_position_fence(supplied.position())?;
        if self.paused {
            return Err(AssignedConsumerMachineError::StaleFetch { supplied });
        }
        self.position.fetch_failed(supplied, failure)
    }

    pub(super) fn fetch_throttle_elapsed(
        &mut self,
        supplied: FetchFence,
        now: Moment,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        self.ensure_position_fence(supplied.position())?;
        if self.paused {
            return Err(AssignedConsumerMachineError::StaleFetch { supplied });
        }
        self.position.fetch_throttle_elapsed(supplied, now)
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
}
