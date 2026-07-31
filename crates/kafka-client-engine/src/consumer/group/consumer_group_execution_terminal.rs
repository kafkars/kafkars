//! Terminal KIP-848 failure effects and exact host observation.

use kafka_client_core::{
    ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatInput,
    ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatRequestKind, LiveGroupAssignment,
};

use super::{
    consumer_group_execution::{ConsumerGroupExecution, ConsumerGroupExecutionError},
    registry_entry::GroupConsumerEntry,
};

pub(super) fn fail_consumer_group_entry(
    entry: &mut GroupConsumerEntry,
    failure: ConsumerGroupHeartbeatFailure,
) -> Result<(), ConsumerGroupExecutionError> {
    let revoked = entry
        .consumer
        .as_mut()
        .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
        .apply_current_failure(failure)?;
    if let Some(assignment) = revoked {
        entry.catalog.commit_consumer_group_revoke(assignment);
    }
    Ok(())
}

impl ConsumerGroupExecution {
    pub(super) fn apply_current_failure(
        &mut self,
        failure: ConsumerGroupHeartbeatFailure,
    ) -> Result<Option<LiveGroupAssignment>, ConsumerGroupExecutionError> {
        let prepared = self
            .prepared
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?;
        let transition = self
            .machine
            .apply(match prepared.kind() {
                ConsumerGroupHeartbeatRequestKind::Join
                | ConsumerGroupHeartbeatRequestKind::Steady => {
                    ConsumerGroupHeartbeatInput::HeartbeatFailed {
                        attempt: prepared.attempt(),
                        failure,
                    }
                }
                ConsumerGroupHeartbeatRequestKind::Leave => {
                    ConsumerGroupHeartbeatInput::LeaveFailed {
                        attempt: prepared.attempt(),
                        failure,
                    }
                }
            })
            .map_err(|error| ConsumerGroupExecutionError::Core(error.kind()))?;
        let mut effects = transition.into_effects().peekable();
        let revoked = match effects.peek() {
            Some(ConsumerGroupHeartbeatEffect::Revoke { .. }) => {
                let Some(ConsumerGroupHeartbeatEffect::Revoke { assignment }) = effects.next()
                else {
                    unreachable!("peeked revoke effect")
                };
                Some(assignment)
            }
            _ => None,
        };
        if !matches!(
            effects.next(),
            Some(ConsumerGroupHeartbeatEffect::Fatal { .. })
        ) || effects.next().is_some()
        {
            return Err(ConsumerGroupExecutionError::EffectShape);
        }
        self.prepared = None;
        Ok(revoked)
    }

    pub(super) fn close_locally(
        &mut self,
    ) -> Result<Option<LiveGroupAssignment>, ConsumerGroupExecutionError> {
        if self.heartbeat_call.is_some() || self.topic_identity_call.is_some() {
            return Err(ConsumerGroupExecutionError::EffectShape);
        }
        let transition = self
            .machine
            .apply(ConsumerGroupHeartbeatInput::Close)
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

    pub(super) fn unsettled(&self) -> usize {
        usize::from(self.machine.in_flight().is_some())
    }

    pub(super) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.prepared
            .map(|prepared| prepared.deadline().core())
            .or_else(|| self.machine.schedule().map(|schedule| schedule.deadline()))
    }
}
