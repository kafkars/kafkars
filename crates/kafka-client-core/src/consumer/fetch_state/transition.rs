//! Fetch-revision allocation and exact phase installation primitives.

use super::super::{
    AssignedConsumerEffect, AssignedConsumerMachineError, AssignedTopicPartition, FetchFence,
    FetchRevision, NextFetchOffset, PositionFence,
    fetch_throttle::FetchThrottle,
    position_state::{PartitionPosition, PositionPhase},
};
use crate::{Deadline, Moment};

impl PartitionPosition {
    pub(in crate::consumer) fn fetch_throttle_elapsed(
        &mut self,
        supplied: FetchFence,
        now: Moment,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        let PositionPhase::FetchThrottled(throttle) = &self.phase else {
            return Err(AssignedConsumerMachineError::FetchThrottleNotPending { fence: supplied });
        };
        throttle.ensure_elapsed(supplied, now)?;
        let next_offset = throttle.next_offset();
        self.phase = PositionPhase::Fetching {
            fence: supplied,
            next_offset,
        };
        Ok(AssignedConsumerEffect::FetchReady {
            fence: supplied,
            next_offset,
        })
    }

    pub(in crate::consumer) fn start_fetch(
        &mut self,
        position: PositionFence,
        next_offset: NextFetchOffset,
        partition: AssignedTopicPartition,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        let (fence, next_revision) = self.plan_fetch(position, partition)?;
        Ok(self.install_fetch(fence, next_revision, next_offset))
    }

    pub(super) fn plan_fetch(
        &self,
        position: PositionFence,
        partition: AssignedTopicPartition,
    ) -> Result<(FetchFence, FetchRevision), AssignedConsumerMachineError> {
        let revision = self.next_fetch_revision;
        let next_revision = revision
            .checked_next()
            .ok_or(AssignedConsumerMachineError::FetchRevisionExhausted { partition })?;
        Ok((FetchFence::new(position, revision), next_revision))
    }

    pub(super) fn install_fetch(
        &mut self,
        fence: FetchFence,
        next_revision: FetchRevision,
        next_offset: NextFetchOffset,
    ) -> AssignedConsumerEffect {
        self.next_fetch_revision = next_revision;
        self.phase = PositionPhase::Fetching { fence, next_offset };
        AssignedConsumerEffect::FetchReady { fence, next_offset }
    }

    pub(super) fn install_throttle(
        &mut self,
        fence: FetchFence,
        next_revision: FetchRevision,
        next_offset: NextFetchOffset,
        deadline: Deadline,
    ) {
        self.next_fetch_revision = next_revision;
        self.phase =
            PositionPhase::FetchThrottled(FetchThrottle::new(fence, next_offset, deadline));
    }

    #[cfg(test)]
    pub(in crate::consumer) fn replace_next_fetch_revision_for_test(
        &mut self,
        revision: FetchRevision,
    ) {
        self.next_fetch_revision = revision;
    }
}
