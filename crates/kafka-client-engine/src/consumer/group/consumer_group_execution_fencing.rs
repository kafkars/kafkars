//! Engine ownership transfer from one fenced KIP-848 steady heartbeat to its recovery Join.

use kafka_client_core::{
    ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatInput,
    ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatRequestKind, LiveGroupAssignment, Moment,
};

use crate::clock::MonotonicClock;

use super::consumer_group_execution::{
    CONSUMER_GROUP_ATTEMPT_TIMEOUT_TICKS, ConsumerGroupExecution, ConsumerGroupExecutionError,
    ConsumerGroupRediscoveryState, PreparedConsumerGroupHeartbeat,
};
use super::{
    consumer_group_close::position_failure_allows_consumer_group_leave,
    registry_entry::{GroupConsumerEntry, GroupConsumerEntryState},
};

pub(super) fn consumer_group_heartbeat_is_ready(entry: &GroupConsumerEntry) -> bool {
    let leave_is_closing = entry.state == GroupConsumerEntryState::Closing
        && entry.consumer.as_ref().is_some_and(|execution| {
            execution
                .prepared()
                .is_some_and(|prepared| prepared.kind() == ConsumerGroupHeartbeatRequestKind::Leave)
        });
    (entry.is_active() || leave_is_closing)
        && (entry.fault.is_none() || position_failure_allows_consumer_group_leave(entry))
        && entry.consumer.as_ref().is_some_and(|execution| {
            consumer_group_execution_is_ready(execution)
                && execution.heartbeat_call().is_none()
                && join_assignment_is_retired(entry, execution)
        })
}

fn join_assignment_is_retired(
    entry: &GroupConsumerEntry,
    execution: &ConsumerGroupExecution,
) -> bool {
    execution.prepared().is_none_or(|prepared| {
        prepared.kind() != ConsumerGroupHeartbeatRequestKind::Join
            || (entry.consumer_revocation.is_none() && entry.catalog.live_assignment().is_none())
    })
}

pub(super) fn consumer_group_execution_is_ready(execution: &ConsumerGroupExecution) -> bool {
    execution.prepared().is_some()
        && execution.machine().retry_schedule().is_none()
        && execution.topic_identity_call().is_none()
        && execution.topic_identities().is_complete()
        && execution.rediscovery_state().permits_submission()
}

impl ConsumerGroupExecution {
    pub(super) fn recover_current_fenced_membership(
        &mut self,
        now: Moment,
        clock: &MonotonicClock,
        failure: ConsumerGroupHeartbeatFailure,
    ) -> Result<Option<LiveGroupAssignment>, ConsumerGroupExecutionError> {
        let prepared = self
            .prepared
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?;
        if prepared.kind() != ConsumerGroupHeartbeatRequestKind::Steady
            || self.rediscovery != ConsumerGroupRediscoveryState::Open
        {
            return Err(ConsumerGroupExecutionError::EffectShape);
        }
        let recovery = if prepared.deadline().core().is_elapsed_at(now) {
            None
        } else {
            let cycle = self
                .cycle
                .and_then(kafka_client_core::MembershipCycle::checked_next)
                .ok_or(ConsumerGroupExecutionError::EffectShape)?;
            let deadline = now
                .checked_deadline_after(CONSUMER_GROUP_ATTEMPT_TIMEOUT_TICKS)
                .ok_or(ConsumerGroupExecutionError::EffectShape)?;
            let deadline = clock
                .operation_deadline(deadline)
                .map_err(|_error| ConsumerGroupExecutionError::EffectShape)?;
            Some((cycle, deadline))
        };
        let transition = self
            .machine
            .apply(ConsumerGroupHeartbeatInput::RecoverFencedMembership {
                attempt: prepared.attempt(),
                now,
                failure,
            })
            .map_err(|error| ConsumerGroupExecutionError::Core(error.kind()))?;
        let mut effects = transition.into_effects();
        let first = effects.next();
        let (revoked, terminal) = match first {
            Some(ConsumerGroupHeartbeatEffect::Revoke { assignment }) => {
                if assignment.group_id() != self.machine.group_id()
                    || Some(assignment.member_id()) != prepared.member_id()
                    || Some(assignment.assignment_generation()) != prepared.assignment_generation()
                {
                    return Err(ConsumerGroupExecutionError::EffectShape);
                }
                (Some(assignment), effects.next())
            }
            Some(effect) if prepared.assignment_generation().is_none() => (None, Some(effect)),
            _ => return Err(ConsumerGroupExecutionError::EffectShape),
        };
        match (terminal, recovery) {
            (
                Some(ConsumerGroupHeartbeatEffect::Submit {
                    group_id,
                    attempt,
                    kind,
                    member_id,
                    member_epoch,
                    assignment_generation,
                    deadline,
                }),
                Some((cycle, mapped_deadline)),
            ) if group_id == self.machine.group_id()
                && kind == ConsumerGroupHeartbeatRequestKind::Join
                && member_id == prepared.member_id()
                && member_id.is_some()
                && member_epoch.is_none()
                && assignment_generation.is_none()
                && attempt.member_epoch().is_none()
                && deadline == mapped_deadline.core() =>
            {
                self.prepared = Some(PreparedConsumerGroupHeartbeat {
                    attempt,
                    kind,
                    member_id,
                    member_epoch,
                    assignment_generation,
                    deadline: mapped_deadline,
                });
                self.cycle = Some(cycle);
                self.clear_rediscovery();
            }
            (Some(ConsumerGroupHeartbeatEffect::Fatal { fatal }), None)
                if fatal.attempt() == prepared.attempt()
                    && fatal.failure() == ConsumerGroupHeartbeatFailure::DeadlineElapsed
                    && self.machine.phase() == ConsumerGroupHeartbeatPhase::Fatal =>
            {
                self.prepared = None;
                self.clear_rediscovery();
            }
            _ => return Err(ConsumerGroupExecutionError::EffectShape),
        }
        if effects.next().is_some() {
            return Err(ConsumerGroupExecutionError::EffectShape);
        }
        Ok(revoked)
    }
}
