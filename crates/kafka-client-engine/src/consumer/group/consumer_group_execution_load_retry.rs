//! Coordinator-load retry scheduling for one prepared KIP-848 heartbeat.

use kafka_client_core::{
    ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatInput,
    ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatRequestKind,
    ConsumerGroupHeartbeatRetryCause, ConsumerGroupHeartbeatRetrySchedule, LiveGroupAssignment,
    Moment,
};

use super::super::consumer_group_execution::{
    ConsumerGroupExecution, ConsumerGroupExecutionError, ConsumerGroupRediscoveryState,
    PreparedConsumerGroupHeartbeat,
};

#[derive(Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ConsumerGroupCoordinatorLoadRetryTurn {
    Idle,
    Scheduled {
        schedule: ConsumerGroupHeartbeatRetrySchedule,
    },
    SubmissionReady,
    Terminal {
        kind: ConsumerGroupHeartbeatRequestKind,
        revoked: Option<LiveGroupAssignment>,
    },
}

impl ConsumerGroupExecution {
    pub(in crate::consumer::group) fn schedule_current_coordinator_load_retry(
        &mut self,
        now: Moment,
        failure: ConsumerGroupHeartbeatFailure,
    ) -> Result<ConsumerGroupCoordinatorLoadRetryTurn, ConsumerGroupExecutionError> {
        let prepared = self
            .prepared
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?;
        if !self.rediscovery_state().permits_submission() {
            return Err(ConsumerGroupExecutionError::EffectShape);
        }
        let transition = self
            .machine
            .apply(ConsumerGroupHeartbeatInput::RetryCoordinatorLoad {
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
            (Some(ConsumerGroupHeartbeatEffect::ArmCoordinatorLoadRetry { schedule }), None)
                if schedule.attempt() == prepared.attempt()
                    && schedule.kind() == prepared.kind()
                    && schedule.cause() == ConsumerGroupHeartbeatRetryCause::CoordinatorLoad
                    && schedule.not_before().tick() > now.tick()
                    && schedule.not_before().tick() <= schedule.deadline().tick()
                    && schedule.deadline() == prepared.deadline().core()
                    && self.machine.retry_schedule() == Some(schedule) =>
            {
                Ok(ConsumerGroupCoordinatorLoadRetryTurn::Scheduled { schedule })
            }
            terminal => self.apply_coordinator_load_terminal(prepared, terminal),
        }
    }

    pub(in crate::consumer::group) fn prepare_due_coordinator_load_retry(
        &mut self,
        now: Moment,
    ) -> Result<ConsumerGroupCoordinatorLoadRetryTurn, ConsumerGroupExecutionError> {
        let Some(schedule) = self.machine.retry_schedule() else {
            return Ok(ConsumerGroupCoordinatorLoadRetryTurn::Idle);
        };
        if !schedule.not_before().is_elapsed_at(now) {
            return Ok(ConsumerGroupCoordinatorLoadRetryTurn::Idle);
        }
        if !retry_cause_matches_rediscovery(schedule.cause(), self.rediscovery_state()) {
            return Err(ConsumerGroupExecutionError::EffectShape);
        }
        let prepared = self
            .prepared
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?;
        let transition = self
            .machine
            .apply(ConsumerGroupHeartbeatInput::CoordinatorLoadRetryDue { schedule, now })
            .map_err(|error| ConsumerGroupExecutionError::Core(error.kind()))?;
        let mut effects = transition.into_effects();
        let first = effects.next();
        let second = effects.next();
        if effects.next().is_some() {
            return Err(ConsumerGroupExecutionError::EffectShape);
        }
        match (first, second) {
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
                None,
            ) if group_id == self.machine.group_id()
                && attempt == prepared.attempt()
                && kind == prepared.kind()
                && member_id == prepared.member_id()
                && member_epoch == prepared.member_epoch()
                && assignment_generation == prepared.assignment_generation()
                && deadline == prepared.deadline().core()
                && retry_cause_matches_rediscovery(schedule.cause(), self.rediscovery_state())
                && self.machine.retry_schedule().is_none() =>
            {
                Ok(ConsumerGroupCoordinatorLoadRetryTurn::SubmissionReady)
            }
            terminal => self.apply_coordinator_load_terminal(prepared, terminal),
        }
    }

    fn apply_coordinator_load_terminal(
        &mut self,
        prepared: PreparedConsumerGroupHeartbeat,
        effects: (
            Option<ConsumerGroupHeartbeatEffect>,
            Option<ConsumerGroupHeartbeatEffect>,
        ),
    ) -> Result<ConsumerGroupCoordinatorLoadRetryTurn, ConsumerGroupExecutionError> {
        let revoked = match effects {
            (Some(ConsumerGroupHeartbeatEffect::Fatal { fatal }), None)
                if fatal.attempt() == prepared.attempt()
                    && fatal.failure() == ConsumerGroupHeartbeatFailure::DeadlineElapsed =>
            {
                None
            }
            (
                Some(ConsumerGroupHeartbeatEffect::Revoke { assignment }),
                Some(ConsumerGroupHeartbeatEffect::Fatal { fatal }),
            ) if fatal.attempt() == prepared.attempt()
                && fatal.failure() == ConsumerGroupHeartbeatFailure::DeadlineElapsed =>
            {
                Some(assignment)
            }
            _ => return Err(ConsumerGroupExecutionError::EffectShape),
        };
        if self.machine.phase() != ConsumerGroupHeartbeatPhase::Fatal
            || self.machine.retry_schedule().is_some()
        {
            return Err(ConsumerGroupExecutionError::EffectShape);
        }
        self.prepared = None;
        self.clear_rediscovery();
        Ok(ConsumerGroupCoordinatorLoadRetryTurn::Terminal {
            kind: prepared.kind(),
            revoked,
        })
    }
}

const fn retry_cause_matches_rediscovery(
    cause: ConsumerGroupHeartbeatRetryCause,
    rediscovery: ConsumerGroupRediscoveryState,
) -> bool {
    matches!(
        (cause, rediscovery),
        (
            ConsumerGroupHeartbeatRetryCause::CoordinatorLoad,
            ConsumerGroupRediscoveryState::Open
        ) | (
            ConsumerGroupHeartbeatRetryCause::Rediscovery,
            ConsumerGroupRediscoveryState::AwaitingInvalidationAdmission
                | ConsumerGroupRediscoveryState::ReplacementAdmitted
        )
    )
}
