//! Position-generation owner and join point for resolution and Fetch phases.

use super::{
    AssignedConsumerEffect, AssignedConsumerMachineError, AssignedTopicPartition, FetchFence,
    FetchRevision, NextFetchOffset, PositionEpoch, PositionFence, PositionOwnership,
    PositionResolutionAttemptFailure, StartPosition,
    fetch_throttle::FetchThrottle,
    position::{RetainedResolutionActivation, RetainedResolutionPlan},
    position_resolution::{PositionResolution, ResolutionActivation},
};
use crate::{Deadline, Moment};

#[derive(Debug)]
pub(super) struct PartitionPosition {
    epoch: PositionEpoch,
    pub(super) next_fetch_revision: FetchRevision,
    pub(super) phase: PositionPhase,
}

#[derive(Debug)]
pub(super) enum PositionPhase {
    Resolution(PositionResolution),
    Ready(NextFetchOffset),
    Fetching {
        fence: FetchFence,
        next_offset: NextFetchOffset,
    },
    FetchThrottled(FetchThrottle),
    FetchFailed(FetchFence),
}

impl PartitionPosition {
    pub(super) const fn new(position: StartPosition) -> Self {
        let phase = match position {
            position @ (StartPosition::Beginning | StartPosition::End) => {
                PositionPhase::Resolution(PositionResolution::new(position))
            }
            StartPosition::Offset(next_offset) => PositionPhase::Ready(next_offset),
        };
        Self {
            epoch: PositionEpoch::initial(),
            next_fetch_revision: FetchRevision::initial(),
            phase,
        }
    }

    pub(super) const fn epoch(&self) -> PositionEpoch {
        self.epoch
    }

    pub(super) fn position_ownership(
        &self,
        fence: PositionFence,
    ) -> Result<PositionOwnership, AssignedConsumerMachineError> {
        match &self.phase {
            PositionPhase::Resolution(resolution) => resolution.ownership(fence),
            PositionPhase::Ready(_)
            | PositionPhase::Fetching { .. }
            | PositionPhase::FetchThrottled(_)
            | PositionPhase::FetchFailed(_) => Ok(PositionOwnership::Superseded),
        }
    }

    #[cfg(test)]
    pub(super) fn replace_epoch_for_test(&mut self, epoch: PositionEpoch) {
        self.epoch = epoch;
    }

    pub(super) fn replace(&mut self, position: StartPosition) {
        self.phase = match position {
            position @ (StartPosition::Beginning | StartPosition::End) => {
                PositionPhase::Resolution(PositionResolution::new(position))
            }
            StartPosition::Offset(next_offset) => PositionPhase::Ready(next_offset),
        };
    }

    pub(super) fn activate(
        &mut self,
        fence: PositionFence,
        partition: AssignedTopicPartition,
        now: Moment,
        deadline: Deadline,
    ) -> Result<Option<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        if let Some(effect) = self.activate_fetch(fence, partition, now)? {
            return Ok(Some(effect));
        }
        let activation = match &mut self.phase {
            PositionPhase::Resolution(resolution) => resolution.activate(fence, now, deadline),
            PositionPhase::Ready(_)
            | PositionPhase::Fetching { .. }
            | PositionPhase::FetchThrottled(_)
            | PositionPhase::FetchFailed(_) => return Ok(None),
        };
        self.apply_resolution_activation(activation, fence, partition)
    }

    pub(in crate::consumer) fn plan_retained_resolution_activation(
        &self,
        fence: PositionFence,
        now: Moment,
        deadline: Deadline,
    ) -> Option<RetainedResolutionPlan> {
        let PositionPhase::Resolution(resolution) = &self.phase else {
            return None;
        };
        resolution.plan_retained_activation(fence, now, deadline)
    }

    pub(in crate::consumer) fn install_retained_resolution_activation(
        &mut self,
        activation: RetainedResolutionActivation,
    ) -> AssignedConsumerEffect {
        let PositionPhase::Resolution(resolution) = &mut self.phase else {
            unreachable!("retained resolution plan preserves its preflighted phase");
        };
        resolution.install_retained_activation(activation)
    }

    pub(super) fn resolve(
        &mut self,
        fence: PositionFence,
        next_offset: NextFetchOffset,
        now: Moment,
        throttle_ticks: u64,
        partition: AssignedTopicPartition,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        let PositionPhase::Resolution(resolution) = &mut self.phase else {
            return Err(AssignedConsumerMachineError::PositionResolutionNotPending { fence });
        };
        let activation = resolution.resolve(fence, next_offset, now, throttle_ticks)?;
        self.apply_resolution_activation(activation, fence, partition)?
            .ok_or(AssignedConsumerMachineError::PositionResolutionNotPending { fence })
    }

    pub(super) fn fail(
        &mut self,
        fence: PositionFence,
        now: Moment,
        failure: PositionResolutionAttemptFailure,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        self.resolution_mut(fence)?
            .terminal_failure(fence, now, failure)
    }

    pub(super) fn resolution_deadline_elapsed(
        &mut self,
        fence: PositionFence,
        now: Moment,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        self.resolution_mut(fence)?.deadline_elapsed(fence, now)
    }

    pub(super) fn throttle_elapsed(
        &mut self,
        fence: PositionFence,
        now: Moment,
        partition: AssignedTopicPartition,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        let PositionPhase::Resolution(resolution) = &mut self.phase else {
            return Err(AssignedConsumerMachineError::PositionThrottleNotPending { fence });
        };
        let next_offset = resolution.throttle_elapsed(fence, now)?;
        self.start_fetch(fence, next_offset, partition)
    }

    pub(super) fn fence(
        &mut self,
        partition: AssignedTopicPartition,
    ) -> Result<(), AssignedConsumerMachineError> {
        let next = self.plan_fence(partition)?;
        self.install_preflighted_fence(next);
        Ok(())
    }

    pub(super) fn plan_fence(
        &self,
        partition: AssignedTopicPartition,
    ) -> Result<PositionEpoch, AssignedConsumerMachineError> {
        self.epoch
            .checked_next()
            .ok_or(AssignedConsumerMachineError::PositionEpochExhausted { partition })
    }

    pub(super) fn install_preflighted_fence(&mut self, next: PositionEpoch) {
        match &mut self.phase {
            PositionPhase::Resolution(resolution) => resolution.fence(),
            PositionPhase::Fetching { next_offset, .. } => {
                self.phase = PositionPhase::Ready(*next_offset);
            }
            PositionPhase::Ready(_)
            | PositionPhase::FetchThrottled(_)
            | PositionPhase::FetchFailed(_) => {}
        }
        self.epoch = next;
        self.next_fetch_revision = FetchRevision::initial();
    }

    fn apply_resolution_activation(
        &mut self,
        activation: ResolutionActivation,
        fence: PositionFence,
        partition: AssignedTopicPartition,
    ) -> Result<Option<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        match activation {
            ResolutionActivation::Effect(effect) => Ok(Some(effect)),
            ResolutionActivation::FetchReady(next_offset) => {
                self.start_fetch(fence, next_offset, partition).map(Some)
            }
            ResolutionActivation::None => Ok(None),
        }
    }

    fn resolution_mut(
        &mut self,
        fence: PositionFence,
    ) -> Result<&mut PositionResolution, AssignedConsumerMachineError> {
        let PositionPhase::Resolution(resolution) = &mut self.phase else {
            return Err(AssignedConsumerMachineError::PositionResolutionNotPending { fence });
        };
        Ok(resolution)
    }
}
