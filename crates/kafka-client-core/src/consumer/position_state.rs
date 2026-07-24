//! Sole mutation owner for one partition's position epoch and fetch revision.

use super::{
    AssignedConsumerEffect, AssignedConsumerMachineError, AssignedTopicPartition, FetchFence,
    FetchRevision, NextFetchOffset, PositionEpoch, PositionFence, StartPosition,
    position_resolution::{PositionResolution, ResolutionActivation},
};
use crate::{Deadline, Moment};

#[derive(Debug)]
pub(super) struct PartitionPosition {
    epoch: PositionEpoch,
    next_fetch_revision: FetchRevision,
    phase: PositionPhase,
}

#[derive(Debug)]
enum PositionPhase {
    Resolution(PositionResolution),
    Ready(NextFetchOffset),
    Fetching {
        fence: FetchFence,
        next_offset: NextFetchOffset,
    },
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

    pub(super) fn replace(&mut self, position: StartPosition) {
        self.phase = match position {
            position @ (StartPosition::Beginning | StartPosition::End) => {
                PositionPhase::Resolution(PositionResolution::new(position))
            }
            StartPosition::Offset(next_offset) => PositionPhase::Ready(next_offset),
        };
    }

    pub(super) fn advance_and_activate(
        &mut self,
        supplied: FetchFence,
        next_offset: NextFetchOffset,
        partition: AssignedTopicPartition,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        let PositionPhase::Fetching {
            fence,
            next_offset: requested,
        } = self.phase
        else {
            return Err(AssignedConsumerMachineError::StaleFetch { supplied });
        };
        if fence != supplied {
            return Err(AssignedConsumerMachineError::StaleFetch { supplied });
        }
        if next_offset < requested {
            return Err(AssignedConsumerMachineError::OffsetRegression {
                requested,
                observed: next_offset,
            });
        }
        self.start_fetch(supplied.position(), next_offset, partition)
    }

    pub(super) fn activate(
        &mut self,
        fence: PositionFence,
        partition: AssignedTopicPartition,
        now: Moment,
        deadline: Deadline,
    ) -> Result<Option<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        if let PositionPhase::Ready(next_offset) = &self.phase {
            let next_offset = *next_offset;
            return self.start_fetch(fence, next_offset, partition).map(Some);
        }
        let activation = match &mut self.phase {
            PositionPhase::Resolution(resolution) => resolution.activate(fence, now, deadline),
            PositionPhase::Ready(_) | PositionPhase::Fetching { .. } => return Ok(None),
        };
        self.apply_resolution_activation(activation, fence, partition)
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
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        self.resolution_mut(fence)?.terminal_failure(fence, now)
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

    fn start_fetch(
        &mut self,
        position: PositionFence,
        next_offset: NextFetchOffset,
        partition: AssignedTopicPartition,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        let revision = self.next_fetch_revision;
        let next_revision = revision
            .checked_next()
            .ok_or(AssignedConsumerMachineError::FetchRevisionExhausted { partition })?;
        let fence = FetchFence::new(position, revision);
        self.next_fetch_revision = next_revision;
        self.phase = PositionPhase::Fetching { fence, next_offset };
        Ok(AssignedConsumerEffect::FetchReady { fence, next_offset })
    }

    pub(super) fn fence(
        &mut self,
        partition: AssignedTopicPartition,
    ) -> Result<(), AssignedConsumerMachineError> {
        let next = self
            .epoch
            .checked_next()
            .ok_or(AssignedConsumerMachineError::PositionEpochExhausted { partition })?;
        match &mut self.phase {
            PositionPhase::Resolution(resolution) => resolution.fence(),
            PositionPhase::Fetching { next_offset, .. } => {
                self.phase = PositionPhase::Ready(*next_offset);
            }
            PositionPhase::Ready(_) => {}
        }
        self.epoch = next;
        self.next_fetch_revision = FetchRevision::initial();
        Ok(())
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

    #[cfg(test)]
    pub(super) fn replace_next_fetch_revision_for_test(&mut self, revision: FetchRevision) {
        self.next_fetch_revision = revision;
    }
}
