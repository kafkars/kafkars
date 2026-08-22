//! Broker-paced heartbeat preparation and local assignment-cycle fencing.

use kafka_client_core::{
    AssignmentGeneration, ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatInput,
    ConsumerGroupHeartbeatRequestKind, ConsumerGroupMemberEpoch, MemberId, MembershipCycle, Moment,
};

use crate::clock::MonotonicClock;

use super::consumer_group_execution::{
    CONSUMER_GROUP_ATTEMPT_TIMEOUT_TICKS, ConsumerGroupExecution, ConsumerGroupExecutionError,
    ConsumerGroupRediscoveryState, PreparedConsumerGroupHeartbeat,
};

#[path = "consumer_group_execution_load_retry.rs"]
mod load_retry;
pub(super) use load_retry::ConsumerGroupCoordinatorLoadRetryTurn;

impl ConsumerGroupExecution {
    pub(super) fn prepare_due_heartbeat(
        &mut self,
        now: Moment,
        clock: &MonotonicClock,
    ) -> Result<bool, ConsumerGroupExecutionError> {
        if self.prepared.is_some() || self.heartbeat_call.is_some() {
            return Ok(false);
        }
        if self.rediscovery_state() != ConsumerGroupRediscoveryState::Open {
            return Err(ConsumerGroupExecutionError::EffectShape);
        }
        let Some(schedule) = self.machine.schedule() else {
            return Ok(false);
        };
        if !schedule.deadline().is_elapsed_at(now) {
            return Ok(false);
        }
        let predicted_deadline = now
            .checked_deadline_after(CONSUMER_GROUP_ATTEMPT_TIMEOUT_TICKS)
            .ok_or(ConsumerGroupExecutionError::EffectShape)?;
        let mapped = clock
            .operation_deadline(predicted_deadline)
            .map_err(|_error| ConsumerGroupExecutionError::EffectShape)?;
        let transition = self
            .machine
            .apply(ConsumerGroupHeartbeatInput::HeartbeatDue { schedule, now })
            .map_err(|error| ConsumerGroupExecutionError::Core(error.kind()))?;
        let mut effects = transition.into_effects();
        let Some(ConsumerGroupHeartbeatEffect::Submit {
            group_id,
            attempt,
            kind,
            member_id,
            member_epoch,
            assignment_generation,
            deadline,
        }) = effects.next()
        else {
            return Err(ConsumerGroupExecutionError::EffectShape);
        };
        if group_id != self.machine.group_id()
            || attempt != schedule.attempt()
            || kind != ConsumerGroupHeartbeatRequestKind::Steady
            || member_id.is_none()
            || member_epoch != attempt.member_epoch()
            || assignment_generation != schedule.assignment_generation()
            || deadline != predicted_deadline
            || effects.next().is_some()
        {
            return Err(ConsumerGroupExecutionError::EffectShape);
        }
        self.prepared = Some(PreparedConsumerGroupHeartbeat {
            attempt,
            kind,
            member_id,
            member_epoch,
            assignment_generation,
            deadline: mapped,
        });
        Ok(true)
    }

    pub(super) fn prepare_assignment_retired_ack(
        &mut self,
        now: Moment,
        clock: &MonotonicClock,
        member_id: MemberId,
        member_epoch: ConsumerGroupMemberEpoch,
        assignment_generation: AssignmentGeneration,
    ) -> Result<(), ConsumerGroupExecutionError> {
        if self.prepared.is_some()
            || self.heartbeat_call.is_some()
            || self.rediscovery_state() != ConsumerGroupRediscoveryState::Open
        {
            return Err(ConsumerGroupExecutionError::EffectShape);
        }
        let predicted_deadline = now
            .checked_deadline_after(CONSUMER_GROUP_ATTEMPT_TIMEOUT_TICKS)
            .ok_or(ConsumerGroupExecutionError::EffectShape)?;
        let mapped = clock
            .operation_deadline(predicted_deadline)
            .map_err(|_error| ConsumerGroupExecutionError::EffectShape)?;
        let transition = self
            .machine
            .apply(ConsumerGroupHeartbeatInput::AssignmentRetired {
                now,
                member_id,
                member_epoch,
                assignment_generation,
            })
            .map_err(|error| ConsumerGroupExecutionError::Core(error.kind()))?;
        let mut effects = transition.into_effects();
        let Some(ConsumerGroupHeartbeatEffect::Submit {
            group_id,
            attempt,
            kind,
            member_id: effect_member_id,
            member_epoch: effect_member_epoch,
            assignment_generation: effect_assignment_generation,
            deadline,
        }) = effects.next()
        else {
            return Err(ConsumerGroupExecutionError::EffectShape);
        };
        if effects.next().is_some()
            || group_id != self.machine.group_id()
            || kind != ConsumerGroupHeartbeatRequestKind::Steady
            || effect_member_id != Some(member_id)
            || effect_member_epoch != Some(member_epoch)
            || attempt.member_epoch() != Some(member_epoch)
            || effect_assignment_generation.is_some()
            || deadline != predicted_deadline
            || self.machine.live_assignment().is_some()
            || self.machine.pending_assignment().is_none()
        {
            return Err(ConsumerGroupExecutionError::EffectShape);
        }
        self.prepared = Some(PreparedConsumerGroupHeartbeat {
            attempt,
            kind,
            member_id: effect_member_id,
            member_epoch: effect_member_epoch,
            assignment_generation: effect_assignment_generation,
            deadline: mapped,
        });
        Ok(())
    }

    pub(super) fn next_reconcile_cycle(
        &self,
        replaces_live_assignment: bool,
    ) -> Option<MembershipCycle> {
        let current = self.cycle?;
        if replaces_live_assignment {
            current.checked_next()
        } else {
            Some(current)
        }
    }

    pub(super) fn commit_reconcile_cycle(&mut self, cycle: MembershipCycle) {
        self.cycle = Some(cycle);
    }
}
