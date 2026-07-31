//! Exact epoch-minus-one preparation and terminal application for KIP-848 close.

use kafka_client_core::{
    ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatInput, ConsumerGroupHeartbeatPhase,
    ConsumerGroupHeartbeatRequestKind, LiveGroupAssignment, Moment,
};

use crate::clock::OperationDeadline;

use super::consumer_group_execution::{
    ConsumerGroupExecution, ConsumerGroupExecutionError, PreparedConsumerGroupHeartbeat,
};

impl ConsumerGroupExecution {
    pub(super) fn prepare_leave(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
    ) -> Result<bool, ConsumerGroupExecutionError> {
        if self.prepared.is_some() || self.heartbeat_call.is_some() {
            return Ok(false);
        }
        let transition = self
            .machine
            .apply(ConsumerGroupHeartbeatInput::BeginLeave {
                now,
                deadline: deadline.core(),
            })
            .map_err(|error| ConsumerGroupExecutionError::Core(error.kind()))?;
        let mut effects = transition.into_effects();
        let Some(effect) = effects.next() else {
            return if self.machine.phase() == ConsumerGroupHeartbeatPhase::Closed {
                Ok(true)
            } else {
                Err(ConsumerGroupExecutionError::EffectShape)
            };
        };
        let ConsumerGroupHeartbeatEffect::Submit {
            group_id,
            attempt,
            kind,
            member_id,
            member_epoch,
            assignment_generation,
            deadline: core_deadline,
        } = effect
        else {
            return Err(ConsumerGroupExecutionError::EffectShape);
        };
        if effects.next().is_some()
            || group_id != self.machine.group_id()
            || kind != ConsumerGroupHeartbeatRequestKind::Leave
            || member_id.is_none()
            || member_epoch != attempt.member_epoch()
            || assignment_generation.is_none()
            || core_deadline != deadline.core()
        {
            return Err(ConsumerGroupExecutionError::EffectShape);
        }
        self.prepared = Some(PreparedConsumerGroupHeartbeat {
            attempt,
            kind,
            member_id,
            member_epoch,
            assignment_generation,
            deadline,
        });
        Ok(true)
    }

    pub(super) fn apply_leave_success(
        &mut self,
    ) -> Result<Option<LiveGroupAssignment>, ConsumerGroupExecutionError> {
        let prepared = self
            .prepared
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?;
        if prepared.kind() != ConsumerGroupHeartbeatRequestKind::Leave {
            return Err(ConsumerGroupExecutionError::EffectShape);
        }
        let transition = self
            .machine
            .apply(ConsumerGroupHeartbeatInput::LeaveSucceeded {
                attempt: prepared.attempt(),
            })
            .map_err(|error| ConsumerGroupExecutionError::Core(error.kind()))?;
        let mut effects = transition.into_effects();
        let revoked = match effects.next() {
            Some(ConsumerGroupHeartbeatEffect::Revoke { assignment }) => Some(assignment),
            None => None,
            Some(_) => return Err(ConsumerGroupExecutionError::EffectShape),
        };
        if effects.next().is_some() || self.machine.phase() != ConsumerGroupHeartbeatPhase::Closed {
            return Err(ConsumerGroupExecutionError::EffectShape);
        }
        self.prepared = None;
        Ok(revoked)
    }
}
