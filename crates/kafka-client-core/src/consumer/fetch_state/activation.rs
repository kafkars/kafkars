//! Atomic activation of retained or newly resolved Fetch positions.

use super::super::{
    AssignedConsumerEffect, AssignedConsumerMachineError, AssignedTopicPartition, FetchFence,
    FetchRevision, NextFetchOffset, PositionFence,
    position_state::{PartitionPosition, PositionPhase},
};
use crate::{Deadline, Moment};

pub(in crate::consumer) enum RetainedFetchActivation {
    Fetch {
        fence: FetchFence,
        next_revision: FetchRevision,
        next_offset: NextFetchOffset,
    },
    Throttle {
        fence: FetchFence,
        next_revision: FetchRevision,
        next_offset: NextFetchOffset,
        deadline: Deadline,
    },
}

impl RetainedFetchActivation {
    pub(in crate::consumer) fn install(
        self,
        position: &mut PartitionPosition,
    ) -> AssignedConsumerEffect {
        match self {
            Self::Fetch {
                fence,
                next_revision,
                next_offset,
            } => position.install_fetch(fence, next_revision, next_offset),
            Self::Throttle {
                fence,
                next_revision,
                next_offset,
                deadline,
            } => {
                position.install_throttle(fence, next_revision, next_offset, deadline);
                AssignedConsumerEffect::ArmFetchThrottle { fence, deadline }
            }
        }
    }
}

impl PartitionPosition {
    pub(in crate::consumer) fn plan_retained_fetch_activation(
        &self,
        position: PositionFence,
        partition: AssignedTopicPartition,
        next_offset: NextFetchOffset,
    ) -> Result<RetainedFetchActivation, AssignedConsumerMachineError> {
        let (fence, next_revision) = self.plan_fetch(position, partition)?;
        Ok(RetainedFetchActivation::Fetch {
            fence,
            next_revision,
            next_offset,
        })
    }

    pub(in crate::consumer) fn plan_retained_activation(
        &self,
        position: PositionFence,
        partition: AssignedTopicPartition,
        now: Moment,
    ) -> Result<Option<RetainedFetchActivation>, AssignedConsumerMachineError> {
        let plan = match &self.phase {
            PositionPhase::Ready(next_offset) => {
                Some(self.plan_retained_fetch_activation(position, partition, *next_offset)?)
            }
            PositionPhase::FetchThrottled(throttle) => {
                let (fence, next_revision) = self.plan_fetch(position, partition)?;
                let next_offset = throttle.next_offset();
                if throttle.deadline().is_elapsed_at(now) {
                    Some(RetainedFetchActivation::Fetch {
                        fence,
                        next_revision,
                        next_offset,
                    })
                } else {
                    Some(RetainedFetchActivation::Throttle {
                        fence,
                        next_revision,
                        next_offset,
                        deadline: throttle.deadline(),
                    })
                }
            }
            PositionPhase::Resolution(_)
            | PositionPhase::Fetching { .. }
            | PositionPhase::FetchFailed(_) => {
                return Err(AssignedConsumerMachineError::PositionNotRetained { partition });
            }
        };
        Ok(plan)
    }

    pub(in crate::consumer) fn start_resolved_assignment_fetch(
        &mut self,
        position: PositionFence,
        next_offset: NextFetchOffset,
        throttle_deadline: Option<Deadline>,
    ) -> AssignedConsumerEffect {
        let revision = FetchRevision::initial();
        let next_revision = FetchRevision::after_initial();
        let fence = FetchFence::new(position, revision);
        match throttle_deadline {
            Some(deadline) => {
                self.install_throttle(fence, next_revision, next_offset, deadline);
                AssignedConsumerEffect::ArmFetchThrottle { fence, deadline }
            }
            None => self.install_fetch(fence, next_revision, next_offset),
        }
    }

    pub(in crate::consumer) fn activate_fetch(
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
            | PositionPhase::FetchFailed(_) => Ok(None),
        }
    }
}
