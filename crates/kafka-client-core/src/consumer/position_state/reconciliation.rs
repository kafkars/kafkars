//! Preflighted re-fencing and activation of retained partition-position owners.

use super::{PartitionPosition, PositionPhase};
use crate::consumer::{
    AssignedConsumerEffect, AssignedConsumerMachineError, AssignedTopicPartition, AssignmentEpoch,
    FetchFence, FetchRevision, NextFetchOffset, PositionEpoch, PositionFence,
    PositionResolutionFailure, StartPosition, fetch_throttle::FetchThrottle,
    position::RetainedResolutionActivation, position_resolution::ResolutionState,
};
use crate::{Deadline, Moment};

#[derive(Clone, Copy)]
pub(in crate::consumer) struct RetainedAssignmentPositionPlan {
    next_epoch: PositionEpoch,
    install: RetainedAssignmentPositionInstall,
    effect: Option<AssignedConsumerEffect>,
}

impl RetainedAssignmentPositionPlan {
    pub(in crate::consumer) const fn has_effect(self) -> bool {
        self.effect.is_some()
    }

    pub(in crate::consumer) const fn suspension_fence(
        self,
        assignment_epoch: AssignmentEpoch,
        partition: AssignedTopicPartition,
    ) -> PositionFence {
        PositionFence::new(assignment_epoch, partition, self.next_epoch)
    }
}

#[derive(Clone, Copy)]
enum RetainedAssignmentPositionInstall {
    Inert,
    Resolve {
        fence: PositionFence,
        position: StartPosition,
        deadline: Deadline,
    },
    ResolutionFailed {
        fence: PositionFence,
    },
    PositionThrottled,
    Fetch {
        fence: FetchFence,
        next_offset: NextFetchOffset,
    },
    FetchThrottled {
        fence: FetchFence,
        next_offset: NextFetchOffset,
        deadline: Deadline,
    },
}

impl PartitionPosition {
    pub(in crate::consumer) fn plan_assignment_reconciliation(
        &self,
        new_assignment_epoch: AssignmentEpoch,
        partition: AssignedTopicPartition,
        paused: bool,
        now: Moment,
    ) -> Result<RetainedAssignmentPositionPlan, AssignedConsumerMachineError> {
        let next_epoch = self.plan_fence(partition)?;
        if paused {
            return Ok(inert(next_epoch));
        }
        let position = PositionFence::new(new_assignment_epoch, partition, next_epoch);
        let (install, effect) = match &self.phase {
            PositionPhase::Resolution(resolution) => match resolution.state {
                ResolutionState::Awaiting(_) | ResolutionState::Failed => {
                    return Ok(inert(next_epoch));
                }
                ResolutionState::Resolving {
                    position: _,
                    deadline,
                } if deadline.is_elapsed_at(now) => (
                    RetainedAssignmentPositionInstall::ResolutionFailed { fence: position },
                    AssignedConsumerEffect::PositionResolutionFailed {
                        fence: position,
                        failure: PositionResolutionFailure::DeadlineElapsed,
                    },
                ),
                ResolutionState::Resolving {
                    position: start,
                    deadline,
                } => (
                    RetainedAssignmentPositionInstall::Resolve {
                        fence: position,
                        position: start,
                        deadline,
                    },
                    AssignedConsumerEffect::ResolvePosition {
                        fence: position,
                        position: start,
                        deadline,
                    },
                ),
                ResolutionState::Throttled {
                    next_offset,
                    deadline,
                } if deadline.is_elapsed_at(now) => fetch(position, next_offset),
                ResolutionState::Throttled {
                    next_offset: _,
                    deadline,
                } => (
                    RetainedAssignmentPositionInstall::PositionThrottled,
                    AssignedConsumerEffect::ArmPositionThrottle {
                        fence: position,
                        deadline,
                    },
                ),
            },
            PositionPhase::Ready(next_offset) | PositionPhase::Fetching { next_offset, .. } => {
                fetch(position, *next_offset)
            }
            PositionPhase::FetchThrottled(throttle) if throttle.deadline().is_elapsed_at(now) => {
                fetch(position, throttle.next_offset())
            }
            PositionPhase::FetchThrottled(throttle) => {
                let fence = FetchFence::new(position, FetchRevision::initial());
                (
                    RetainedAssignmentPositionInstall::FetchThrottled {
                        fence,
                        next_offset: throttle.next_offset(),
                        deadline: throttle.deadline(),
                    },
                    AssignedConsumerEffect::ArmFetchThrottle {
                        fence,
                        deadline: throttle.deadline(),
                    },
                )
            }
            PositionPhase::FetchFailed(_) => return Ok(inert(next_epoch)),
        };
        Ok(RetainedAssignmentPositionPlan {
            next_epoch,
            install,
            effect: Some(effect),
        })
    }

    pub(in crate::consumer) fn install_assignment_reconciliation(
        &mut self,
        plan: RetainedAssignmentPositionPlan,
    ) -> Option<AssignedConsumerEffect> {
        self.install_preflighted_fence(plan.next_epoch);
        match plan.install {
            RetainedAssignmentPositionInstall::Inert
            | RetainedAssignmentPositionInstall::PositionThrottled => {}
            RetainedAssignmentPositionInstall::Resolve {
                fence,
                position,
                deadline,
            } => {
                let PositionPhase::Resolution(resolution) = &mut self.phase else {
                    unreachable!("preflighted symbolic resolution retains its phase");
                };
                let _ =
                    resolution.install_retained_activation(RetainedResolutionActivation::Resolve {
                        fence,
                        position,
                        deadline,
                    });
            }
            RetainedAssignmentPositionInstall::ResolutionFailed { fence } => {
                let PositionPhase::Resolution(resolution) = &mut self.phase else {
                    unreachable!("preflighted symbolic resolution retains its phase");
                };
                let _ = resolution.install_retained_activation(
                    RetainedResolutionActivation::DeadlineElapsed { fence },
                );
            }
            RetainedAssignmentPositionInstall::Fetch { fence, next_offset } => {
                self.next_fetch_revision = FetchRevision::after_initial();
                self.phase = PositionPhase::Fetching { fence, next_offset };
            }
            RetainedAssignmentPositionInstall::FetchThrottled {
                fence,
                next_offset,
                deadline,
            } => {
                self.next_fetch_revision = FetchRevision::after_initial();
                self.phase =
                    PositionPhase::FetchThrottled(FetchThrottle::new(fence, next_offset, deadline));
            }
        }
        plan.effect
    }
}

const fn inert(next_epoch: PositionEpoch) -> RetainedAssignmentPositionPlan {
    RetainedAssignmentPositionPlan {
        next_epoch,
        install: RetainedAssignmentPositionInstall::Inert,
        effect: None,
    }
}

fn fetch(
    position: PositionFence,
    next_offset: NextFetchOffset,
) -> (RetainedAssignmentPositionInstall, AssignedConsumerEffect) {
    let fence = FetchFence::new(position, FetchRevision::initial());
    (
        RetainedAssignmentPositionInstall::Fetch { fence, next_offset },
        AssignedConsumerEffect::FetchReady { fence, next_offset },
    )
}
