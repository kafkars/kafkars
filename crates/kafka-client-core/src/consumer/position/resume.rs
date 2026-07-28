//! Immutable plans for atomically resuming retained symbolic position work.

use crate::{
    Deadline, Moment,
    consumer::{
        NextFetchOffset, PositionFence, StartPosition,
        position_resolution::{PositionResolution, ResolutionState},
    },
};

#[derive(Clone, Copy)]
pub(in crate::consumer) enum RetainedResolutionActivation {
    DeadlineElapsed {
        fence: PositionFence,
    },
    Resolve {
        fence: PositionFence,
        position: StartPosition,
        deadline: Deadline,
    },
    ArmThrottle {
        fence: PositionFence,
        deadline: Deadline,
    },
}

#[derive(Clone, Copy)]
pub(in crate::consumer) enum RetainedResolutionPlan {
    Install(RetainedResolutionActivation),
    Fetch(NextFetchOffset),
}

impl PositionResolution {
    pub(in crate::consumer) const fn plan_retained_activation(
        &self,
        fence: PositionFence,
        now: Moment,
        deadline: Deadline,
    ) -> Option<RetainedResolutionPlan> {
        match self.state {
            ResolutionState::Awaiting(_) if deadline.is_elapsed_at(now) => {
                Some(RetainedResolutionPlan::Install(
                    RetainedResolutionActivation::DeadlineElapsed { fence },
                ))
            }
            ResolutionState::Awaiting(position) => Some(RetainedResolutionPlan::Install(
                RetainedResolutionActivation::Resolve {
                    fence,
                    position,
                    deadline,
                },
            )),
            ResolutionState::Throttled {
                next_offset,
                deadline,
            } if deadline.is_elapsed_at(now) => Some(RetainedResolutionPlan::Fetch(next_offset)),
            ResolutionState::Throttled { deadline, .. } => Some(RetainedResolutionPlan::Install(
                RetainedResolutionActivation::ArmThrottle { fence, deadline },
            )),
            ResolutionState::Resolving { .. } | ResolutionState::Failed => None,
        }
    }
}
