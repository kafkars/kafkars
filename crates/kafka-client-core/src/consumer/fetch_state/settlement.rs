//! Exact ownership queries and terminal settlement of active Fetch attempts.

use super::super::{
    AssignedConsumerEffect, AssignedConsumerMachineError, AssignedTopicPartition, FetchFailure,
    FetchFence, FetchOwnership, FetchRecords, FetchThrottleFailure, NextFetchOffset,
    position_state::{PartitionPosition, PositionPhase},
};
use crate::Moment;

impl PartitionPosition {
    pub(in crate::consumer) fn fetch_ownership(
        &self,
        supplied: FetchFence,
    ) -> Result<FetchOwnership, AssignedConsumerMachineError> {
        match &self.phase {
            PositionPhase::Fetching { fence, .. } if *fence == supplied => {
                Ok(FetchOwnership::Active)
            }
            PositionPhase::Fetching { fence, .. } if supplied.revision() < fence.revision() => {
                Ok(FetchOwnership::Superseded)
            }
            PositionPhase::FetchThrottled(throttle)
                if supplied.revision() < throttle.fence().revision() =>
            {
                Ok(FetchOwnership::Superseded)
            }
            PositionPhase::FetchFailed(last_issued)
                if supplied.revision() <= last_issued.revision() =>
            {
                Ok(FetchOwnership::Superseded)
            }
            PositionPhase::Fetching { .. }
            | PositionPhase::FetchThrottled(_)
            | PositionPhase::FetchFailed(_)
            | PositionPhase::Resolution(_)
            | PositionPhase::Ready(_) => Err(AssignedConsumerMachineError::StaleFetch { supplied }),
        }
    }

    pub(in crate::consumer) fn advance(
        &mut self,
        supplied: FetchFence,
        records: FetchRecords,
        next_offset: NextFetchOffset,
        now: Moment,
        throttle_ticks: u64,
        partition: AssignedTopicPartition,
    ) -> Result<Vec<AssignedConsumerEffect>, AssignedConsumerMachineError> {
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
        let progress = if throttle_ticks == 0 {
            self.install_fetch(next_fence, next_revision, next_offset)
        } else if let Some(deadline) = now.checked_deadline_after(throttle_ticks) {
            self.install_throttle(next_fence, next_revision, next_offset, deadline);
            AssignedConsumerEffect::ArmFetchThrottle {
                fence: next_fence,
                deadline,
            }
        } else {
            self.phase = PositionPhase::FetchFailed(supplied);
            AssignedConsumerEffect::FetchThrottleFailed {
                fence: supplied,
                failure: FetchThrottleFailure::DeadlineOverflow,
            }
        };
        let mut effects = Vec::with_capacity(usize::from(records == FetchRecords::Deliverable) + 1);
        if records == FetchRecords::Deliverable {
            effects.push(AssignedConsumerEffect::AuthorizeFetchDelivery {
                fence: supplied,
                next_offset,
            });
        }
        effects.push(progress);
        Ok(effects)
    }

    pub(in crate::consumer) fn fetch_failed(
        &mut self,
        supplied: FetchFence,
        failure: FetchFailure,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        let PositionPhase::Fetching { fence, .. } = self.phase else {
            return Err(AssignedConsumerMachineError::StaleFetch { supplied });
        };
        if fence != supplied {
            return Err(AssignedConsumerMachineError::StaleFetch { supplied });
        }
        self.phase = PositionPhase::FetchFailed(supplied);
        Ok(AssignedConsumerEffect::FetchFailed {
            fence: supplied,
            failure,
        })
    }

    pub(in crate::consumer) fn fetch_retry(
        &mut self,
        supplied: FetchFence,
        partition: AssignedTopicPartition,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        let PositionPhase::Fetching { fence, next_offset } = self.phase else {
            return Err(AssignedConsumerMachineError::StaleFetch { supplied });
        };
        if fence != supplied {
            return Err(AssignedConsumerMachineError::StaleFetch { supplied });
        }
        let (replacement, next_revision) = self.plan_fetch(supplied.position(), partition)?;
        Ok(self.install_fetch(replacement, next_revision, next_offset))
    }
}
