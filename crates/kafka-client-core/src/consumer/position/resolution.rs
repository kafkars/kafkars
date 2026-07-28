//! Fenced position-resolution terminal transitions for one partition.

use super::AssignedPartitionState;
use crate::{
    Moment,
    consumer::{
        AssignedConsumerEffect, AssignedConsumerMachineError, NextFetchOffset, PositionFence,
        PositionResolutionAttemptFailure,
    },
};

impl AssignedPartitionState {
    pub(in crate::consumer) fn position_resolved(
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

    pub(in crate::consumer) fn position_resolution_failed(
        &mut self,
        fence: PositionFence,
        now: Moment,
        failure: PositionResolutionAttemptFailure,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        self.ensure_position_fence(fence)?;
        self.position.fail(fence, now, failure)
    }

    pub(in crate::consumer) fn position_resolution_deadline_elapsed(
        &mut self,
        fence: PositionFence,
        now: Moment,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        self.ensure_position_fence(fence)?;
        self.position.resolution_deadline_elapsed(fence, now)
    }

    pub(in crate::consumer) fn position_throttle_elapsed(
        &mut self,
        fence: PositionFence,
        now: Moment,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        self.ensure_position_fence(fence)?;
        self.position.throttle_elapsed(fence, now, self.partition)
    }
}
