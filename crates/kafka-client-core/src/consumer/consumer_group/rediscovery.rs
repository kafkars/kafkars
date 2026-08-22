//! Bounded coordinator rediscovery for one unsettled KIP-848 heartbeat attempt.

use crate::Moment;

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatErrorKind,
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatPhase,
    ConsumerGroupHeartbeatRequestKind, ConsumerGroupHeartbeatRetryCause,
    ConsumerGroupHeartbeatTransition, load_retry::retry_schedule,
};

impl ConsumerGroupHeartbeatMachine {
    pub(super) fn rediscovery_failed(
        &mut self,
        schedule: super::ConsumerGroupHeartbeatRetrySchedule,
        failure: ConsumerGroupHeartbeatFailure,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        let kind = self.retry_kind(schedule.attempt())?;
        if self.retry_schedule != Some(schedule)
            || schedule.kind() != kind
            || schedule.cause() != ConsumerGroupHeartbeatRetryCause::Rediscovery
        {
            return Err(ConsumerGroupHeartbeatErrorKind::CoordinatorLoadRetryScheduleMismatch);
        }
        Ok(self.fail(schedule.attempt(), failure))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one retry transition validates and transfers the complete heartbeat ownership tuple"
    )]
    pub(super) fn retry_heartbeat(
        &mut self,
        attempt: ConsumerGroupHeartbeatAttempt,
        now: Moment,
        failure: ConsumerGroupHeartbeatFailure,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        if !matches!(
            self.phase,
            ConsumerGroupHeartbeatPhase::Joining
                | ConsumerGroupHeartbeatPhase::Heartbeating
                | ConsumerGroupHeartbeatPhase::Leaving
        ) {
            return Err(ConsumerGroupHeartbeatErrorKind::InvalidPhase);
        }
        if self.in_flight != Some(attempt) {
            return Err(ConsumerGroupHeartbeatErrorKind::AttemptMismatch);
        }
        if self.retry_schedule.is_some() {
            return Err(ConsumerGroupHeartbeatErrorKind::CoordinatorLoadRetryPending);
        }
        if !matches!(
            failure,
            ConsumerGroupHeartbeatFailure::CoordinatorUnavailable
                | ConsumerGroupHeartbeatFailure::Broker(15 | 16)
        ) {
            return Err(ConsumerGroupHeartbeatErrorKind::FailureNotRetryable);
        }
        let deadline = self
            .deadline
            .ok_or(ConsumerGroupHeartbeatErrorKind::InvariantViolation)?;
        if deadline.is_elapsed_at(now) {
            return Ok(self.fail(attempt, ConsumerGroupHeartbeatFailure::DeadlineElapsed));
        }
        let (kind, member_id, member_epoch, assignment_generation) = match self.phase {
            ConsumerGroupHeartbeatPhase::Joining => {
                if attempt.member_epoch().is_some()
                    || self.member_epoch.is_some()
                    || self.live_assignment.is_some()
                    || self.pending_assignment.is_some()
                {
                    return Err(ConsumerGroupHeartbeatErrorKind::InvariantViolation);
                }
                (
                    ConsumerGroupHeartbeatRequestKind::Join,
                    self.member_id,
                    None,
                    None,
                )
            }
            ConsumerGroupHeartbeatPhase::Heartbeating => {
                let member_id = self
                    .member_id
                    .ok_or(ConsumerGroupHeartbeatErrorKind::InvariantViolation)?;
                let member_epoch = self
                    .member_epoch
                    .ok_or(ConsumerGroupHeartbeatErrorKind::InvariantViolation)?;
                let assignment_generation = match self.live_assignment.as_ref() {
                    Some(assignment)
                        if assignment.group_id() == self.group_id
                            && assignment.member_id() == member_id =>
                    {
                        Some(assignment.assignment_generation())
                    }
                    None if self.pending_assignment.as_ref().is_some_and(|assignment| {
                        assignment.group_id() == self.group_id
                            && assignment.member_id() == member_id
                    }) =>
                    {
                        None
                    }
                    None if self.pending_assignment.is_none() => None,
                    _ => return Err(ConsumerGroupHeartbeatErrorKind::InvariantViolation),
                };
                if attempt.member_epoch() != Some(member_epoch) {
                    return Err(ConsumerGroupHeartbeatErrorKind::InvariantViolation);
                }
                (
                    ConsumerGroupHeartbeatRequestKind::Steady,
                    Some(member_id),
                    Some(member_epoch),
                    assignment_generation,
                )
            }
            ConsumerGroupHeartbeatPhase::Leaving => {
                let member_id = self
                    .member_id
                    .ok_or(ConsumerGroupHeartbeatErrorKind::InvariantViolation)?;
                let member_epoch = self
                    .member_epoch
                    .ok_or(ConsumerGroupHeartbeatErrorKind::InvariantViolation)?;
                let assignment_generation = match self.live_assignment.as_ref() {
                    Some(assignment)
                        if assignment.group_id() == self.group_id
                            && assignment.member_id() == member_id =>
                    {
                        Some(assignment.assignment_generation())
                    }
                    None if self.pending_assignment.is_none() => None,
                    _ => return Err(ConsumerGroupHeartbeatErrorKind::InvariantViolation),
                };
                if attempt.member_epoch() != Some(member_epoch) {
                    return Err(ConsumerGroupHeartbeatErrorKind::InvariantViolation);
                }
                (
                    ConsumerGroupHeartbeatRequestKind::Leave,
                    Some(member_id),
                    Some(member_epoch),
                    assignment_generation,
                )
            }
            _ => return Err(ConsumerGroupHeartbeatErrorKind::InvalidPhase),
        };

        let (replacement, next_sequence) = match self.reserve_attempt(member_epoch) {
            Ok(reserved) => reserved,
            Err(ConsumerGroupHeartbeatErrorKind::AttemptExhausted) => {
                return Ok(self.fail(attempt, ConsumerGroupHeartbeatFailure::Execution));
            }
            Err(error) => return Err(error),
        };
        let schedule = retry_schedule(
            replacement,
            kind,
            ConsumerGroupHeartbeatRetryCause::Rediscovery,
            now,
            deadline,
        );
        self.next_sequence = next_sequence;
        self.in_flight = Some(replacement);
        self.retry_schedule = Some(schedule);
        Ok(ConsumerGroupHeartbeatTransition::two(
            ConsumerGroupHeartbeatEffect::Rediscover {
                group_id: self.group_id,
                attempt: replacement,
                kind,
                member_id,
                member_epoch,
                assignment_generation,
                deadline,
            },
            ConsumerGroupHeartbeatEffect::ArmRediscoveryRetry { schedule },
        ))
    }
}
