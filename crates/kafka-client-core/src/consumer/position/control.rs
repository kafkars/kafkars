//! Pause, resume, seek, and close fencing for one assigned partition.

use super::{AssignedPartitionState, RetainedResolutionPlan, RetainedResumePlan};
use crate::{
    Deadline, Moment,
    consumer::{
        AssignedConsumerEffect, AssignedConsumerMachineError, PositionEpoch, StartPosition,
    },
};

impl AssignedPartitionState {
    pub(in crate::consumer) fn plan_pause(
        &self,
    ) -> Result<Option<PositionEpoch>, AssignedConsumerMachineError> {
        if self.paused {
            Ok(None)
        } else {
            self.position.plan_fence(self.partition).map(Some)
        }
    }

    pub(in crate::consumer) fn install_planned_pause(
        &mut self,
        next_epoch: Option<PositionEpoch>,
    ) -> Option<AssignedConsumerEffect> {
        let next_epoch = next_epoch?;
        self.position.install_preflighted_fence(next_epoch);
        self.paused = true;
        Some(AssignedConsumerEffect::Suspend {
            fence: self.position_fence(),
        })
    }

    pub(in crate::consumer) fn plan_retained_resume(
        &self,
        now: Moment,
        resolution_deadline: Deadline,
    ) -> Result<RetainedResumePlan, AssignedConsumerMachineError> {
        if !self.paused {
            return Ok(RetainedResumePlan::AlreadyResumed);
        }
        let fence = self.position_fence();
        if let Some(plan) =
            self.position
                .plan_retained_resolution_activation(fence, now, resolution_deadline)
        {
            return match plan {
                RetainedResolutionPlan::Install(activation) => {
                    Ok(RetainedResumePlan::ResumeResolution(activation))
                }
                RetainedResolutionPlan::Fetch(next_offset) => self
                    .position
                    .plan_retained_fetch_activation(fence, self.partition, next_offset)
                    .map(Some)
                    .map(RetainedResumePlan::ResumeFetch),
            };
        }
        self.position
            .plan_retained_activation(fence, self.partition, now)
            .map(RetainedResumePlan::ResumeFetch)
    }

    pub(in crate::consumer) fn install_planned_resume(
        &mut self,
        plan: RetainedResumePlan,
    ) -> Option<AssignedConsumerEffect> {
        match plan {
            RetainedResumePlan::AlreadyResumed => None,
            RetainedResumePlan::ResumeFetch(activation) => {
                self.paused = false;
                activation.map(|activation| activation.install(&mut self.position))
            }
            RetainedResumePlan::ResumeResolution(activation) => {
                self.paused = false;
                Some(
                    self.position
                        .install_retained_resolution_activation(activation),
                )
            }
        }
    }

    pub(in crate::consumer) fn plan_close(
        &self,
    ) -> Result<PositionEpoch, AssignedConsumerMachineError> {
        self.position.plan_fence(self.partition)
    }

    pub(in crate::consumer) fn suspend_for_close(
        &mut self,
        next_epoch: PositionEpoch,
    ) -> AssignedConsumerEffect {
        self.position.install_preflighted_fence(next_epoch);
        self.paused = true;
        AssignedConsumerEffect::Suspend {
            fence: self.position_fence(),
        }
    }

    pub(in crate::consumer) fn pause(
        &mut self,
    ) -> Result<Option<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        if self.paused {
            return Ok(None);
        }
        self.fence_position()?;
        self.paused = true;
        Ok(Some(AssignedConsumerEffect::Suspend {
            fence: self.position_fence(),
        }))
    }

    pub(in crate::consumer) fn resume(
        &mut self,
        now: Moment,
        deadline: Deadline,
    ) -> Result<Option<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        if !self.paused {
            return Ok(None);
        }
        self.paused = false;
        self.activate(now, deadline)
    }

    pub(in crate::consumer) fn seek(
        &mut self,
        position: StartPosition,
        now: Moment,
        deadline: Deadline,
    ) -> Result<Vec<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        self.fence_position()?;
        self.position.replace(position);
        let mut effects = vec![AssignedConsumerEffect::Suspend {
            fence: self.position_fence(),
        }];
        if !self.paused {
            if let Some(effect) = self.activate(now, deadline)? {
                effects.push(effect);
            }
        }
        Ok(effects)
    }
}
