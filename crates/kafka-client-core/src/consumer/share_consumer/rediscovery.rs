//! Bounded share-coordinator rediscovery for one unsettled heartbeat.

use crate::Moment;

use super::{
    ShareGroupHeartbeatAttempt, ShareGroupHeartbeatEffect, ShareGroupHeartbeatErrorKind,
    ShareGroupHeartbeatFailure, ShareGroupHeartbeatMachine, ShareGroupHeartbeatPhase,
    ShareGroupHeartbeatRequestKind, ShareGroupHeartbeatRetryCause,
    ShareGroupHeartbeatRetrySchedule, ShareGroupHeartbeatTransition, retry::retry_schedule,
};

impl ShareGroupHeartbeatMachine {
    pub(super) fn rediscovery_failed(
        &mut self,
        schedule: ShareGroupHeartbeatRetrySchedule,
        failure: ShareGroupHeartbeatFailure,
    ) -> Result<ShareGroupHeartbeatTransition, ShareGroupHeartbeatErrorKind> {
        let kind = self.retry_kind(schedule.attempt())?;
        if self.retry_schedule != Some(schedule)
            || schedule.kind() != kind
            || schedule.cause() != ShareGroupHeartbeatRetryCause::Rediscovery
        {
            return Err(ShareGroupHeartbeatErrorKind::RetryScheduleMismatch);
        }
        Ok(self.fail(schedule.attempt(), failure))
    }

    pub(super) fn retry_heartbeat(
        &mut self,
        attempt: ShareGroupHeartbeatAttempt,
        now: Moment,
        failure: ShareGroupHeartbeatFailure,
    ) -> Result<ShareGroupHeartbeatTransition, ShareGroupHeartbeatErrorKind> {
        if !matches!(
            self.phase,
            ShareGroupHeartbeatPhase::Joining
                | ShareGroupHeartbeatPhase::Heartbeating
                | ShareGroupHeartbeatPhase::Leaving
        ) {
            return Err(ShareGroupHeartbeatErrorKind::InvalidPhase);
        }
        if self.in_flight != Some(attempt) {
            return Err(ShareGroupHeartbeatErrorKind::AttemptMismatch);
        }
        if self.retry_schedule.is_some() {
            return Err(ShareGroupHeartbeatErrorKind::RetryPending);
        }
        if !matches!(
            failure,
            ShareGroupHeartbeatFailure::CoordinatorUnavailable
                | ShareGroupHeartbeatFailure::Broker(15 | 16)
        ) {
            return Err(ShareGroupHeartbeatErrorKind::FailureNotRetryable);
        }
        let deadline = self
            .deadline
            .ok_or(ShareGroupHeartbeatErrorKind::InvariantViolation)?;
        if deadline.is_elapsed_at(now) {
            return Ok(self.fail(attempt, ShareGroupHeartbeatFailure::DeadlineElapsed));
        }
        let kind = match self.phase {
            ShareGroupHeartbeatPhase::Joining => ShareGroupHeartbeatRequestKind::Join,
            ShareGroupHeartbeatPhase::Heartbeating => ShareGroupHeartbeatRequestKind::Steady,
            ShareGroupHeartbeatPhase::Leaving => ShareGroupHeartbeatRequestKind::Leave,
            _ => return Err(ShareGroupHeartbeatErrorKind::InvalidPhase),
        };
        let (member_epoch, assignment_generation) = self.retry_facts(kind)?;
        let (replacement, next_sequence) = match self.reserve_attempt(member_epoch) {
            Ok(reserved) => reserved,
            Err(ShareGroupHeartbeatErrorKind::AttemptExhausted) => {
                return Ok(self.fail(attempt, ShareGroupHeartbeatFailure::Execution));
            }
            Err(error) => return Err(error),
        };
        let schedule = retry_schedule(
            replacement,
            kind,
            ShareGroupHeartbeatRetryCause::Rediscovery,
            now,
            deadline,
        );
        self.next_sequence = next_sequence;
        self.in_flight = Some(replacement);
        self.retry_schedule = Some(schedule);
        Ok(ShareGroupHeartbeatTransition::two(
            ShareGroupHeartbeatEffect::Rediscover {
                group_id: self.group_id,
                member_id: self.member_id,
                attempt: replacement,
                kind,
                member_epoch,
                assignment_generation,
                deadline,
            },
            ShareGroupHeartbeatEffect::ArmRetry { schedule },
        ))
    }
}
