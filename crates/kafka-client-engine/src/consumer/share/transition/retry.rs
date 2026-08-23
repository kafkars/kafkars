//! Two-gate retry timing and original-deadline terminalization.

use kafka_client_core::{
    ShareGroupHeartbeatEffect, ShareGroupHeartbeatInput, ShareGroupHeartbeatRetrySchedule,
};

use super::super::membership::{
    ShareMembershipError, ShareMembershipInterpreter, ShareMembershipRetryDueTurn,
    ShareMembershipRetryGate,
};
use super::map_core;

impl ShareMembershipInterpreter {
    pub(in crate::consumer::share) fn observe_retry_due(
        &mut self,
        schedule: ShareGroupHeartbeatRetrySchedule,
        now: kafka_client_core::Moment,
    ) -> Result<ShareMembershipRetryDueTurn, ShareMembershipError> {
        let transition = self
            .machine
            .apply(ShareGroupHeartbeatInput::RetryDue { schedule, now })
            .map_err(map_core)?;
        if !transition
            .effects()
            .any(|effect| matches!(effect, ShareGroupHeartbeatEffect::Submit { .. }))
        {
            self.consume_terminal(transition)?;
            self.prepared = None;
            self.retry_gate = ShareMembershipRetryGate::Open;
            return Ok(ShareMembershipRetryDueTurn::Terminal);
        }
        validate_retry_submit(self, transition, schedule)?;
        match self.retry_gate {
            ShareMembershipRetryGate::CoordinatorLoad => {
                self.retry_gate = ShareMembershipRetryGate::Open;
            }
            ShareMembershipRetryGate::Rediscovery {
                invalidation_complete,
                ..
            } => {
                self.retry_gate = if invalidation_complete {
                    ShareMembershipRetryGate::Open
                } else {
                    ShareMembershipRetryGate::Rediscovery {
                        retry_due: true,
                        invalidation_complete: false,
                    }
                };
            }
            ShareMembershipRetryGate::Open => return Err(ShareMembershipError::EffectShape),
        }
        Ok(ShareMembershipRetryDueTurn::SubmissionReady)
    }

    pub(in crate::consumer::share) fn expire_prepared_deadline(
        &mut self,
        now: kafka_client_core::Moment,
    ) -> Result<bool, ShareMembershipError> {
        let Some(prepared) = self.prepared else {
            return Ok(false);
        };
        if !prepared.deadline.core().is_elapsed_at(now) {
            return Ok(false);
        }
        if let Some(schedule) = self.machine.retry_schedule() {
            let _turn = self.observe_retry_due(schedule, now)?;
            return Ok(true);
        }
        self.apply_terminal(
            prepared,
            kafka_client_core::ShareGroupHeartbeatFailure::DeadlineElapsed,
        )?;
        Ok(true)
    }

    pub(in crate::consumer::share) fn permit_rediscovery(
        &mut self,
    ) -> Result<(), ShareMembershipError> {
        let ShareMembershipRetryGate::Rediscovery { retry_due, .. } = self.retry_gate else {
            return Err(ShareMembershipError::EffectShape);
        };
        self.retry_gate = if retry_due {
            ShareMembershipRetryGate::Open
        } else {
            ShareMembershipRetryGate::Rediscovery {
                retry_due: false,
                invalidation_complete: true,
            }
        };
        Ok(())
    }
}

fn validate_retry_submit(
    owner: &ShareMembershipInterpreter,
    transition: kafka_client_core::ShareGroupHeartbeatTransition,
    schedule: ShareGroupHeartbeatRetrySchedule,
) -> Result<(), ShareMembershipError> {
    let prepared = owner.prepared.ok_or(ShareMembershipError::EffectShape)?;
    let mut effects = transition.into_effects();
    let Some(ShareGroupHeartbeatEffect::Submit {
        group_id,
        member_id,
        attempt,
        kind,
        member_epoch,
        assignment_generation,
        deadline,
    }) = effects.next()
    else {
        return Err(ShareMembershipError::EffectShape);
    };
    if effects.next().is_some()
        || group_id != owner.machine.group_id()
        || member_id != owner.machine.member_id()
        || attempt != prepared.attempt
        || kind != prepared.kind
        || member_epoch != prepared.member_epoch
        || assignment_generation != prepared.assignment_generation
        || deadline != prepared.deadline.core()
        || schedule.attempt() != prepared.attempt
    {
        return Err(ShareMembershipError::EffectShape);
    }
    Ok(())
}
