//! Initial admission, broker cadence, leave, and local-close transitions.

use kafka_client_core::{ShareGroupHeartbeatEffect, ShareGroupHeartbeatInput};

use crate::clock::{DeadlineCapture, MonotonicClock};

use super::super::{
    membership::{ShareMembershipError, ShareMembershipInterpreter, ShareMembershipRetryGate},
    prepared::PreparedShareGroupHeartbeat,
};

impl ShareMembershipInterpreter {
    pub(in crate::consumer::share) fn begin(
        &mut self,
        capture: DeadlineCapture,
    ) -> Result<(), ShareMembershipError> {
        if self.prepared.is_some() || self.retry_gate != ShareMembershipRetryGate::Open {
            return Err(ShareMembershipError::Occupied);
        }
        let transition = self
            .machine
            .apply(ShareGroupHeartbeatInput::Begin {
                now: capture.now(),
                deadline: capture.deadline(),
            })
            .map_err(map_core)?;
        self.install_submit(transition, capture.operation_deadline(), None)
    }

    pub(in crate::consumer::share) fn prepare_heartbeat_due(
        &mut self,
        now: kafka_client_core::Moment,
        clock: &MonotonicClock,
    ) -> Result<bool, ShareMembershipError> {
        if self.prepared.is_some() || self.retry_gate != ShareMembershipRetryGate::Open {
            return Ok(false);
        }
        let Some(schedule) = self.machine.schedule() else {
            return Ok(false);
        };
        if !schedule.deadline().is_elapsed_at(now) {
            return Ok(false);
        }
        let transition = self
            .machine
            .apply(ShareGroupHeartbeatInput::HeartbeatDue { schedule, now })
            .map_err(map_core)?;
        let deadline = transition
            .effects()
            .find_map(|effect| match effect {
                ShareGroupHeartbeatEffect::Submit { deadline, .. } => Some(*deadline),
                _ => None,
            })
            .ok_or(ShareMembershipError::EffectShape)?;
        let mapped = clock
            .operation_deadline(deadline)
            .map_err(|_error| ShareMembershipError::DeadlineMapping)?;
        self.install_submit(transition, mapped, Some(schedule.attempt()))?;
        Ok(true)
    }

    pub(in crate::consumer::share) fn begin_leave(
        &mut self,
        capture: DeadlineCapture,
    ) -> Result<(), ShareMembershipError> {
        if self.prepared.is_some() || self.retry_gate != ShareMembershipRetryGate::Open {
            return Err(ShareMembershipError::Occupied);
        }
        let transition = self
            .machine
            .apply(ShareGroupHeartbeatInput::BeginLeave {
                now: capture.now(),
                deadline: capture.deadline(),
            })
            .map_err(map_core)?;
        if self.machine.phase() == kafka_client_core::ShareGroupHeartbeatPhase::Closed {
            return consume_close_effects(self, transition);
        }
        self.install_submit(transition, capture.operation_deadline(), None)
    }

    pub(in crate::consumer::share) fn close_locally(&mut self) -> Result<(), ShareMembershipError> {
        let transition = self
            .machine
            .apply(ShareGroupHeartbeatInput::Close)
            .map_err(map_core)?;
        self.prepared = None;
        self.retry_gate = ShareMembershipRetryGate::Open;
        consume_close_effects(self, transition)
    }

    fn install_submit(
        &mut self,
        transition: kafka_client_core::ShareGroupHeartbeatTransition,
        deadline: crate::clock::OperationDeadline,
        expected_attempt: Option<kafka_client_core::ShareGroupHeartbeatAttempt>,
    ) -> Result<(), ShareMembershipError> {
        let mut effects = transition.into_effects();
        let Some(ShareGroupHeartbeatEffect::Submit {
            group_id,
            member_id,
            attempt,
            kind,
            member_epoch,
            assignment_generation,
            deadline: core_deadline,
        }) = effects.next()
        else {
            return Err(ShareMembershipError::EffectShape);
        };
        if effects.next().is_some()
            || group_id != self.machine.group_id()
            || member_id != self.machine.member_id()
            || expected_attempt.is_some_and(|expected| expected != attempt)
            || core_deadline != deadline.core()
        {
            return Err(ShareMembershipError::EffectShape);
        }
        self.prepared = Some(PreparedShareGroupHeartbeat {
            attempt,
            kind,
            member_epoch,
            assignment_generation,
            deadline,
        });
        Ok(())
    }
}

pub(in crate::consumer::share) fn consume_close_effects(
    owner: &mut ShareMembershipInterpreter,
    transition: kafka_client_core::ShareGroupHeartbeatTransition,
) -> Result<(), ShareMembershipError> {
    let mut effects = transition.into_effects();
    match effects.next() {
        Some(ShareGroupHeartbeatEffect::Revoke { assignment })
            if owner.activated_assignment.as_ref() == Some(&assignment) =>
        {
            drop(owner.activated_assignment.take());
            drop(assignment);
        }
        None if owner.activated_assignment.is_none() => {}
        _ => return Err(ShareMembershipError::EffectShape),
    }
    if effects.next().is_some()
        || owner.machine.phase() != kafka_client_core::ShareGroupHeartbeatPhase::Closed
    {
        return Err(ShareMembershipError::EffectShape);
    }
    Ok(())
}

pub(in crate::consumer::share) fn map_core(
    error: kafka_client_core::ShareGroupHeartbeatApplyError,
) -> ShareMembershipError {
    ShareMembershipError::Core(error.kind())
}
