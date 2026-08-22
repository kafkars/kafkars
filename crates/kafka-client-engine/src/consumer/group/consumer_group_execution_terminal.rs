//! Terminal KIP-848 failure effects and exact host observation.

use kafka_client_core::{
    ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatInput,
    ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatRequestKind,
    ConsumerGroupHeartbeatRetryCause, LiveGroupAssignment, Moment,
};

use super::{
    consumer_group_assignment_retirement::stage_consumer_group_revocation,
    consumer_group_execution::{ConsumerGroupExecution, ConsumerGroupExecutionError},
    registry_entry::GroupConsumerEntry,
};

#[derive(Debug, Eq, PartialEq)]
pub(super) enum ConsumerGroupRediscoveryDecision {
    Rediscover,
    Terminal {
        revoked: Option<LiveGroupAssignment>,
        failure: ConsumerGroupHeartbeatFailure,
    },
}

pub(super) fn fail_consumer_group_entry(
    entry: &mut GroupConsumerEntry,
    failure: ConsumerGroupHeartbeatFailure,
) -> Result<(), ConsumerGroupExecutionError> {
    let revoked = entry
        .consumer
        .as_mut()
        .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
        .apply_current_failure(failure)?;
    if revoked.is_none()
        && entry.catalog.current_member_id().is_some()
        && entry.catalog.live_assignment().is_none()
    {
        entry
            .catalog
            .commit_consumer_group_close_without_assignment();
    }
    drop(entry.consumer_reconciliation.take());
    stage_consumer_group_revocation(entry, revoked)?;
    Ok(())
}

impl ConsumerGroupExecution {
    pub(super) fn apply_current_rediscovery(
        &mut self,
        now: Moment,
        failure: ConsumerGroupHeartbeatFailure,
    ) -> Result<ConsumerGroupRediscoveryDecision, ConsumerGroupExecutionError> {
        let prepared = self
            .prepared
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?;
        let transition = self
            .machine
            .apply(ConsumerGroupHeartbeatInput::RetryHeartbeat {
                attempt: prepared.attempt(),
                now,
                failure,
            })
            .map_err(|error| ConsumerGroupExecutionError::Core(error.kind()))?;
        let mut effects = transition.into_effects();
        let first = effects.next();
        let second = effects.next();
        if effects.next().is_some() {
            return Err(ConsumerGroupExecutionError::EffectShape);
        }
        match (first, second) {
            (
                Some(ConsumerGroupHeartbeatEffect::Rediscover {
                    group_id,
                    attempt,
                    kind,
                    member_id,
                    member_epoch,
                    assignment_generation,
                    deadline,
                }),
                Some(ConsumerGroupHeartbeatEffect::ArmRediscoveryRetry { schedule }),
            ) if group_id == self.machine.group_id()
                && attempt != prepared.attempt()
                && kind == prepared.kind()
                && member_id == prepared.member_id()
                && member_epoch == prepared.member_epoch()
                && assignment_generation == prepared.assignment_generation()
                && deadline == prepared.deadline().core()
                && schedule.attempt() == attempt
                && schedule.kind() == kind
                && schedule.cause() == ConsumerGroupHeartbeatRetryCause::Rediscovery
                && schedule.not_before().tick() > now.tick()
                && schedule.not_before().tick() <= deadline.tick()
                && schedule.deadline() == deadline
                && self.machine.in_flight() == Some(attempt)
                && self.machine.retry_schedule() == Some(schedule) =>
            {
                self.prepared = Some(
                    super::consumer_group_execution::PreparedConsumerGroupHeartbeat {
                        attempt,
                        kind,
                        member_id,
                        member_epoch,
                        assignment_generation,
                        deadline: prepared.deadline(),
                    },
                );
                self.await_rediscovery_admission()?;
                Ok(ConsumerGroupRediscoveryDecision::Rediscover)
            }
            (Some(ConsumerGroupHeartbeatEffect::Fatal { fatal }), None)
                if fatal.attempt() == prepared.attempt()
                    && self.machine.phase() == ConsumerGroupHeartbeatPhase::Fatal =>
            {
                self.prepared = None;
                self.clear_rediscovery();
                Ok(ConsumerGroupRediscoveryDecision::Terminal {
                    revoked: None,
                    failure: fatal.failure(),
                })
            }
            (
                Some(ConsumerGroupHeartbeatEffect::Revoke { assignment }),
                Some(ConsumerGroupHeartbeatEffect::Fatal { fatal }),
            ) if fatal.attempt() == prepared.attempt()
                && self.machine.phase() == ConsumerGroupHeartbeatPhase::Fatal =>
            {
                self.prepared = None;
                self.clear_rediscovery();
                Ok(ConsumerGroupRediscoveryDecision::Terminal {
                    revoked: Some(assignment),
                    failure: fatal.failure(),
                })
            }
            _ => Err(ConsumerGroupExecutionError::EffectShape),
        }
    }

    pub(super) fn apply_current_failure(
        &mut self,
        failure: ConsumerGroupHeartbeatFailure,
    ) -> Result<Option<LiveGroupAssignment>, ConsumerGroupExecutionError> {
        let prepared = self
            .prepared
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?;
        let input =
            if let Some(schedule) = self.machine.retry_schedule().filter(|schedule| {
                schedule.cause() == ConsumerGroupHeartbeatRetryCause::Rediscovery
            }) {
                ConsumerGroupHeartbeatInput::RediscoveryFailed { schedule, failure }
            } else {
                match prepared.kind() {
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
                }
            };
        let transition = self
            .machine
            .apply(input)
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
        self.clear_rediscovery();
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
        self.clear_rediscovery();
        Ok(revoked)
    }

    pub(super) fn unsettled(&self) -> usize {
        usize::from(self.machine.in_flight().is_some())
    }

    pub(super) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        [
            self.prepared.map(|prepared| prepared.deadline().core()),
            self.machine
                .retry_schedule()
                .map(kafka_client_core::ConsumerGroupHeartbeatRetrySchedule::not_before),
            self.machine
                .schedule()
                .map(kafka_client_core::ConsumerGroupHeartbeatSchedule::deadline),
        ]
        .into_iter()
        .flatten()
        .min()
    }
}
