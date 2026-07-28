//! Sole owner of one partition's fenced resolution and throttle phase.

use super::{
    AssignedConsumerEffect, AssignedConsumerMachineError, NextFetchOffset, PositionFence,
    PositionOwnership, PositionResolutionAttemptFailure, PositionResolutionFailure, StartPosition,
    position::RetainedResolutionActivation,
};
use crate::{Deadline, Moment};

#[derive(Debug)]
pub(super) struct PositionResolution {
    pub(in crate::consumer) state: ResolutionState,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::consumer) enum ResolutionState {
    Awaiting(StartPosition),
    Resolving {
        position: StartPosition,
        deadline: Deadline,
    },
    Throttled {
        next_offset: NextFetchOffset,
        deadline: Deadline,
    },
    Failed,
}

#[derive(Clone, Copy)]
pub(super) enum ResolutionActivation {
    Effect(AssignedConsumerEffect),
    FetchReady(NextFetchOffset),
    None,
}

impl PositionResolution {
    pub(super) const fn new(position: StartPosition) -> Self {
        Self {
            state: ResolutionState::Awaiting(position),
        }
    }

    pub(super) const fn ownership(
        &self,
        fence: PositionFence,
    ) -> Result<PositionOwnership, AssignedConsumerMachineError> {
        match self.state {
            ResolutionState::Resolving { .. } => Ok(PositionOwnership::Active),
            ResolutionState::Throttled { .. } | ResolutionState::Failed => {
                Ok(PositionOwnership::Superseded)
            }
            ResolutionState::Awaiting(_) => {
                Err(AssignedConsumerMachineError::PositionResolutionNotPending { fence })
            }
        }
    }

    pub(super) fn activate(
        &mut self,
        fence: PositionFence,
        now: Moment,
        deadline: Deadline,
    ) -> ResolutionActivation {
        match self.state {
            ResolutionState::Awaiting(_) if deadline.is_elapsed_at(now) => {
                ResolutionActivation::Effect(
                    self.fail(fence, PositionResolutionFailure::DeadlineElapsed),
                )
            }
            ResolutionState::Awaiting(position) => {
                self.state = ResolutionState::Resolving { position, deadline };
                ResolutionActivation::Effect(AssignedConsumerEffect::ResolvePosition {
                    fence,
                    position,
                    deadline,
                })
            }
            ResolutionState::Throttled {
                next_offset,
                deadline,
            } if deadline.is_elapsed_at(now) => ResolutionActivation::FetchReady(next_offset),
            ResolutionState::Throttled { deadline, .. } => {
                ResolutionActivation::Effect(AssignedConsumerEffect::ArmPositionThrottle {
                    fence,
                    deadline,
                })
            }
            ResolutionState::Resolving { .. } | ResolutionState::Failed => {
                ResolutionActivation::None
            }
        }
    }

    pub(super) fn install_retained_activation(
        &mut self,
        activation: RetainedResolutionActivation,
    ) -> AssignedConsumerEffect {
        match activation {
            RetainedResolutionActivation::DeadlineElapsed { fence } => {
                self.fail(fence, PositionResolutionFailure::DeadlineElapsed)
            }
            RetainedResolutionActivation::Resolve {
                fence,
                position,
                deadline,
            } => {
                self.state = ResolutionState::Resolving { position, deadline };
                AssignedConsumerEffect::ResolvePosition {
                    fence,
                    position,
                    deadline,
                }
            }
            RetainedResolutionActivation::ArmThrottle { fence, deadline } => {
                AssignedConsumerEffect::ArmPositionThrottle { fence, deadline }
            }
        }
    }

    pub(super) fn resolve(
        &mut self,
        fence: PositionFence,
        next_offset: NextFetchOffset,
        now: Moment,
        throttle_ticks: u64,
    ) -> Result<ResolutionActivation, AssignedConsumerMachineError> {
        let ResolutionState::Resolving { deadline, .. } = self.state else {
            return Err(AssignedConsumerMachineError::PositionResolutionNotPending { fence });
        };
        if deadline.is_elapsed_at(now) {
            return Ok(ResolutionActivation::Effect(
                self.fail(fence, PositionResolutionFailure::DeadlineElapsed),
            ));
        }
        if throttle_ticks == 0 {
            return Ok(ResolutionActivation::FetchReady(next_offset));
        }
        let Some(deadline) = now.checked_deadline_after(throttle_ticks) else {
            return Ok(ResolutionActivation::Effect(
                self.fail(fence, PositionResolutionFailure::ThrottleDeadlineOverflow),
            ));
        };
        self.state = ResolutionState::Throttled {
            next_offset,
            deadline,
        };
        Ok(ResolutionActivation::Effect(
            AssignedConsumerEffect::ArmPositionThrottle { fence, deadline },
        ))
    }

    pub(super) fn terminal_failure(
        &mut self,
        fence: PositionFence,
        now: Moment,
        attempt_failure: PositionResolutionAttemptFailure,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        let ResolutionState::Resolving { deadline, .. } = self.state else {
            return Err(AssignedConsumerMachineError::PositionResolutionNotPending { fence });
        };
        let failure = if deadline.is_elapsed_at(now) {
            PositionResolutionFailure::DeadlineElapsed
        } else {
            PositionResolutionFailure::Attempt(attempt_failure)
        };
        Ok(self.fail(fence, failure))
    }

    pub(super) fn deadline_elapsed(
        &mut self,
        fence: PositionFence,
        now: Moment,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        let ResolutionState::Resolving { deadline, .. } = self.state else {
            return Err(AssignedConsumerMachineError::PositionResolutionNotPending { fence });
        };
        if !deadline.is_elapsed_at(now) {
            return Err(
                AssignedConsumerMachineError::PositionResolutionDeadlineNotElapsed {
                    fence,
                    deadline,
                    now,
                },
            );
        }
        Ok(self.fail(fence, PositionResolutionFailure::DeadlineElapsed))
    }

    pub(super) fn throttle_elapsed(
        &self,
        fence: PositionFence,
        now: Moment,
    ) -> Result<NextFetchOffset, AssignedConsumerMachineError> {
        let ResolutionState::Throttled {
            next_offset,
            deadline,
        } = self.state
        else {
            return Err(AssignedConsumerMachineError::PositionThrottleNotPending { fence });
        };
        if !deadline.is_elapsed_at(now) {
            return Err(
                AssignedConsumerMachineError::PositionThrottleDeadlineNotElapsed {
                    fence,
                    deadline,
                    now,
                },
            );
        }
        Ok(next_offset)
    }

    pub(super) fn fence(&mut self) {
        if let ResolutionState::Resolving { position, .. } = self.state {
            self.state = ResolutionState::Awaiting(position);
        }
    }

    fn fail(
        &mut self,
        fence: PositionFence,
        failure: PositionResolutionFailure,
    ) -> AssignedConsumerEffect {
        self.state = ResolutionState::Failed;
        AssignedConsumerEffect::PositionResolutionFailed { fence, failure }
    }
}
