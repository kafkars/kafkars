//! Exact installation of coordinator-load, rediscovery, and fenced-member retries.

use kafka_client_core::{
    ShareGroupHeartbeatEffect, ShareGroupHeartbeatRequestKind, ShareGroupHeartbeatRetryCause,
};

use super::super::{
    membership::{
        ShareMembershipError, ShareMembershipFailureTurn, ShareMembershipInterpreter,
        ShareMembershipRetryGate,
    },
    prepared::PreparedShareGroupHeartbeat,
};

impl ShareMembershipInterpreter {
    pub(super) fn install_retry(
        &mut self,
        transition: kafka_client_core::ShareGroupHeartbeatTransition,
        cause: ShareGroupHeartbeatRetryCause,
    ) -> Result<ShareMembershipFailureTurn, ShareMembershipError> {
        let mut effects = transition.into_effects();
        let Some(ShareGroupHeartbeatEffect::ArmRetry { schedule }) = effects.next() else {
            return Err(ShareMembershipError::EffectShape);
        };
        if effects.next().is_some()
            || schedule.cause() != cause
            || self.prepared.is_none_or(|prepared| {
                schedule.attempt() != prepared.attempt
                    || schedule.kind() != prepared.kind
                    || schedule.deadline() != prepared.deadline.core()
            })
        {
            return Err(ShareMembershipError::EffectShape);
        }
        self.retry_gate = ShareMembershipRetryGate::CoordinatorLoad;
        Ok(ShareMembershipFailureTurn::RetryScheduled(schedule))
    }

    pub(super) fn install_rediscovery(
        &mut self,
        transition: kafka_client_core::ShareGroupHeartbeatTransition,
        clock: &crate::clock::MonotonicClock,
        rejected: PreparedShareGroupHeartbeat,
    ) -> Result<ShareMembershipFailureTurn, ShareMembershipError> {
        let mut effects = transition.into_effects();
        let Some(ShareGroupHeartbeatEffect::Rediscover {
            previous,
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
        let Some(ShareGroupHeartbeatEffect::ArmRetry { schedule }) = effects.next() else {
            return Err(ShareMembershipError::EffectShape);
        };
        let retained_attempt = kind == rejected.kind
            && member_epoch == rejected.member_epoch
            && assignment_generation == rejected.assignment_generation
            && deadline == rejected.deadline.core()
            && previous.is_none();
        let fresh_join = rejected.kind == ShareGroupHeartbeatRequestKind::Steady
            && kind == ShareGroupHeartbeatRequestKind::Join
            && member_epoch.is_none()
            && assignment_generation.is_none()
            && deadline > rejected.deadline.core()
            && previous.as_ref() == self.activated_assignment.as_ref();
        if effects.next().is_some()
            || group_id != self.machine.group_id()
            || member_id != self.machine.member_id()
            || attempt == rejected.attempt
            || (!retained_attempt && !fresh_join)
            || schedule.attempt() != attempt
            || schedule.kind() != kind
            || schedule.cause() != ShareGroupHeartbeatRetryCause::Rediscovery
            || schedule.deadline() != deadline
        {
            return Err(ShareMembershipError::EffectShape);
        }
        let deadline = if fresh_join {
            clock
                .operation_deadline(deadline)
                .map_err(|_error| ShareMembershipError::DeadlineMapping)?
        } else {
            rejected.deadline
        };
        if fresh_join {
            drop(self.activated_assignment.take());
            drop(previous);
        }
        self.prepared = Some(PreparedShareGroupHeartbeat {
            attempt,
            kind,
            member_epoch,
            assignment_generation,
            deadline,
        });
        self.retry_gate = ShareMembershipRetryGate::Rediscovery {
            retry_due: false,
            invalidation_complete: false,
        };
        Ok(ShareMembershipFailureTurn::Rediscovery(schedule))
    }

    pub(super) fn install_rejoin(
        &mut self,
        transition: kafka_client_core::ShareGroupHeartbeatTransition,
        clock: &crate::clock::MonotonicClock,
        rejected: PreparedShareGroupHeartbeat,
    ) -> Result<(), ShareMembershipError> {
        let mut effects = transition.into_effects().peekable();
        if matches!(
            effects.peek(),
            Some(ShareGroupHeartbeatEffect::Revoke { .. })
        ) {
            let Some(ShareGroupHeartbeatEffect::Revoke { assignment }) = effects.next() else {
                unreachable!("peeked exact revoke effect")
            };
            if self.activated_assignment.as_ref() != Some(&assignment) {
                return Err(ShareMembershipError::EffectShape);
            }
            drop(self.activated_assignment.take());
            drop(assignment);
        }
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
            || group_id != self.machine.group_id()
            || member_id != self.machine.member_id()
            || attempt == rejected.attempt
            || kind != ShareGroupHeartbeatRequestKind::Join
            || member_epoch.is_some()
            || assignment_generation.is_some()
        {
            return Err(ShareMembershipError::EffectShape);
        }
        let deadline = clock
            .operation_deadline(deadline)
            .map_err(|_error| ShareMembershipError::DeadlineMapping)?;
        self.prepared = Some(PreparedShareGroupHeartbeat {
            attempt,
            kind,
            member_epoch,
            assignment_generation,
            deadline,
        });
        self.retry_gate = ShareMembershipRetryGate::Open;
        Ok(())
    }
}
