//! Sole owner of atomic successful-Fetch progression and throttle transitions.

use super::{
    AssignedConsumerEffect, AssignedConsumerMachineError, AssignedTopicPartition, FetchFence,
    FetchRevision, FetchThrottleFailure, NextFetchOffset, PositionFence,
    fetch_throttle::FetchThrottle,
    position_state::{PartitionPosition, PositionPhase},
};
use crate::Moment;

impl PartitionPosition {
    pub(super) fn advance(
        &mut self,
        supplied: FetchFence,
        next_offset: NextFetchOffset,
        now: Moment,
        throttle_ticks: u64,
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
        let (next_fence, next_revision) = self.plan_fetch(supplied.position(), partition)?;
        if throttle_ticks == 0 {
            return Ok(self.install_fetch(next_fence, next_revision, next_offset));
        }
        let Some(deadline) = now.checked_deadline_after(throttle_ticks) else {
            self.phase = PositionPhase::FetchFailed;
            return Ok(AssignedConsumerEffect::FetchThrottleFailed {
                fence: supplied,
                failure: FetchThrottleFailure::DeadlineOverflow,
            });
        };
        self.install_throttle(next_fence, next_revision, next_offset, deadline);
        Ok(AssignedConsumerEffect::ArmFetchThrottle {
            fence: next_fence,
            deadline,
        })
    }

    pub(super) fn activate_fetch(
        &mut self,
        position: PositionFence,
        partition: AssignedTopicPartition,
        now: Moment,
    ) -> Result<Option<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        match &self.phase {
            PositionPhase::Ready(next_offset) => {
                let next_offset = *next_offset;
                self.start_fetch(position, next_offset, partition).map(Some)
            }
            PositionPhase::FetchThrottled(throttle) => {
                let next_offset = throttle.next_offset();
                let deadline = throttle.deadline();
                let (fence, next_revision) = self.plan_fetch(position, partition)?;
                if deadline.is_elapsed_at(now) {
                    return Ok(Some(self.install_fetch(fence, next_revision, next_offset)));
                }
                self.install_throttle(fence, next_revision, next_offset, deadline);
                Ok(Some(AssignedConsumerEffect::ArmFetchThrottle {
                    fence,
                    deadline,
                }))
            }
            PositionPhase::Resolution(_)
            | PositionPhase::Fetching { .. }
            | PositionPhase::FetchFailed => Ok(None),
        }
    }

    pub(super) fn fetch_throttle_elapsed(
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

    pub(super) fn start_fetch(
        &mut self,
        position: PositionFence,
        next_offset: NextFetchOffset,
        partition: AssignedTopicPartition,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        let (fence, next_revision) = self.plan_fetch(position, partition)?;
        Ok(self.install_fetch(fence, next_revision, next_offset))
    }

    fn plan_fetch(
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

    fn install_fetch(
        &mut self,
        fence: FetchFence,
        next_revision: FetchRevision,
        next_offset: NextFetchOffset,
    ) -> AssignedConsumerEffect {
        self.next_fetch_revision = next_revision;
        self.phase = PositionPhase::Fetching { fence, next_offset };
        AssignedConsumerEffect::FetchReady { fence, next_offset }
    }

    fn install_throttle(
        &mut self,
        fence: FetchFence,
        next_revision: FetchRevision,
        next_offset: NextFetchOffset,
        deadline: crate::Deadline,
    ) {
        self.next_fetch_revision = next_revision;
        self.phase =
            PositionPhase::FetchThrottled(FetchThrottle::new(fence, next_offset, deadline));
    }

    #[cfg(test)]
    pub(super) fn replace_next_fetch_revision_for_test(&mut self, revision: FetchRevision) {
        self.next_fetch_revision = revision;
    }
}
