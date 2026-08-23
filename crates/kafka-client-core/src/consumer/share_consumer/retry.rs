//! Positive original-deadline scheduling for share heartbeat retries.

use crate::{Deadline, Moment};

use super::{
    ShareGroupHeartbeatAttempt, ShareGroupHeartbeatEffect, ShareGroupHeartbeatErrorKind,
    ShareGroupHeartbeatFailure, ShareGroupHeartbeatMachine, ShareGroupHeartbeatPhase,
    ShareGroupHeartbeatRequestKind, ShareGroupHeartbeatRetryCause,
    ShareGroupHeartbeatRetrySchedule, ShareGroupHeartbeatTransition,
};

const SHARE_GROUP_HEARTBEAT_RETRY_BACKOFF_TICKS: u64 = 100_000_000;

impl ShareGroupHeartbeatMachine {
    pub(super) fn retry_coordinator_load(
        &mut self,
        attempt: ShareGroupHeartbeatAttempt,
        now: Moment,
        failure: ShareGroupHeartbeatFailure,
    ) -> Result<ShareGroupHeartbeatTransition, ShareGroupHeartbeatErrorKind> {
        let kind = self.retry_kind(attempt)?;
        if self.retry_schedule.is_some() {
            return Err(ShareGroupHeartbeatErrorKind::RetryPending);
        }
        if failure != ShareGroupHeartbeatFailure::Broker(14) {
            return Err(ShareGroupHeartbeatErrorKind::FailureNotCoordinatorLoad);
        }
        let deadline = self
            .deadline
            .ok_or(ShareGroupHeartbeatErrorKind::InvariantViolation)?;
        if deadline.is_elapsed_at(now) {
            return Ok(self.fail(attempt, ShareGroupHeartbeatFailure::DeadlineElapsed));
        }
        let schedule = retry_schedule(
            attempt,
            kind,
            ShareGroupHeartbeatRetryCause::CoordinatorLoad,
            now,
            deadline,
        );
        self.retry_schedule = Some(schedule);
        Ok(ShareGroupHeartbeatTransition::one(
            ShareGroupHeartbeatEffect::ArmRetry { schedule },
        ))
    }

    pub(super) fn retry_due(
        &mut self,
        schedule: ShareGroupHeartbeatRetrySchedule,
        now: Moment,
    ) -> Result<ShareGroupHeartbeatTransition, ShareGroupHeartbeatErrorKind> {
        let kind = self.retry_kind(schedule.attempt())?;
        if self.retry_schedule != Some(schedule)
            || schedule.kind() != kind
            || self.deadline != Some(schedule.deadline())
        {
            return Err(ShareGroupHeartbeatErrorKind::RetryScheduleMismatch);
        }
        if !schedule.not_before().is_elapsed_at(now) {
            return Err(ShareGroupHeartbeatErrorKind::RetryNotDue);
        }
        if schedule.deadline().is_elapsed_at(now) {
            return Ok(self.fail(
                schedule.attempt(),
                ShareGroupHeartbeatFailure::DeadlineElapsed,
            ));
        }
        let (member_epoch, assignment_generation) = self.retry_facts(kind)?;
        self.retry_schedule = None;
        Ok(ShareGroupHeartbeatTransition::one(self.submit_effect(
            schedule.attempt(),
            kind,
            member_epoch,
            assignment_generation,
            schedule.deadline(),
        )))
    }

    pub(super) fn retry_kind(
        &self,
        attempt: ShareGroupHeartbeatAttempt,
    ) -> Result<ShareGroupHeartbeatRequestKind, ShareGroupHeartbeatErrorKind> {
        let kind = match self.phase {
            ShareGroupHeartbeatPhase::Joining => ShareGroupHeartbeatRequestKind::Join,
            ShareGroupHeartbeatPhase::Heartbeating => ShareGroupHeartbeatRequestKind::Steady,
            ShareGroupHeartbeatPhase::Leaving => ShareGroupHeartbeatRequestKind::Leave,
            _ => return Err(ShareGroupHeartbeatErrorKind::InvalidPhase),
        };
        if self.in_flight != Some(attempt) {
            return Err(ShareGroupHeartbeatErrorKind::AttemptMismatch);
        }
        Ok(kind)
    }

    pub(super) fn retry_facts(
        &self,
        kind: ShareGroupHeartbeatRequestKind,
    ) -> Result<RetryFacts, ShareGroupHeartbeatErrorKind> {
        if kind == ShareGroupHeartbeatRequestKind::Join {
            if self.member_epoch.is_some()
                || self.live_assignment.is_some()
                || self
                    .in_flight
                    .is_none_or(|attempt| attempt.member_epoch().is_some())
            {
                return Err(ShareGroupHeartbeatErrorKind::InvariantViolation);
            }
            return Ok((None, None));
        }
        let member_epoch = self
            .member_epoch
            .ok_or(ShareGroupHeartbeatErrorKind::InvariantViolation)?;
        if self
            .in_flight
            .is_none_or(|attempt| attempt.member_epoch() != Some(member_epoch))
        {
            return Err(ShareGroupHeartbeatErrorKind::InvariantViolation);
        }
        Ok((Some(member_epoch), self.current_assignment_generation()?))
    }
}

pub(super) fn retry_schedule(
    attempt: ShareGroupHeartbeatAttempt,
    kind: ShareGroupHeartbeatRequestKind,
    cause: ShareGroupHeartbeatRetryCause,
    now: Moment,
    deadline: Deadline,
) -> ShareGroupHeartbeatRetrySchedule {
    let not_before = now
        .checked_deadline_after(SHARE_GROUP_HEARTBEAT_RETRY_BACKOFF_TICKS)
        .map_or(deadline, |backoff| backoff.min(deadline));
    ShareGroupHeartbeatRetrySchedule {
        attempt,
        kind,
        cause,
        not_before,
        deadline,
    }
}

type RetryFacts = (
    Option<super::ShareGroupMemberEpoch>,
    Option<crate::AssignmentGeneration>,
);
