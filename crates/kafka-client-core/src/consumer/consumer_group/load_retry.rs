//! Positive original-deadline KIP-848 heartbeat retry scheduling.

use crate::{Deadline, Moment};

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatErrorKind,
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatPhase,
    ConsumerGroupHeartbeatRequestKind, ConsumerGroupHeartbeatTransition,
};

const CONSUMER_GROUP_HEARTBEAT_RETRY_BACKOFF_TICKS: u64 = 100_000_000;

/// Semantic authority paired with one positive heartbeat retry delay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerGroupHeartbeatRetryCause {
    /// Kafka reported `COORDINATOR_LOAD_IN_PROGRESS` on the retained route.
    CoordinatorLoad,
    /// Kafka rejected a stale coordinator route that must be invalidated first.
    Rediscovery,
}

/// Exact future fence for retrying one retained or freshly replaced request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerGroupHeartbeatRetrySchedule {
    attempt: ConsumerGroupHeartbeatAttempt,
    kind: ConsumerGroupHeartbeatRequestKind,
    cause: ConsumerGroupHeartbeatRetryCause,
    not_before: Deadline,
    deadline: Deadline,
}

impl ConsumerGroupHeartbeatRetrySchedule {
    const fn new(
        attempt: ConsumerGroupHeartbeatAttempt,
        kind: ConsumerGroupHeartbeatRequestKind,
        cause: ConsumerGroupHeartbeatRetryCause,
        not_before: Deadline,
        deadline: Deadline,
    ) -> Self {
        Self {
            attempt,
            kind,
            cause,
            not_before,
            deadline,
        }
    }

    /// Returns the exact request identity authorized after this delay.
    pub const fn attempt(self) -> ConsumerGroupHeartbeatAttempt {
        self.attempt
    }

    /// Returns the unchanged Join, Steady, or Leave request shape.
    pub const fn kind(self) -> ConsumerGroupHeartbeatRequestKind {
        self.kind
    }

    /// Returns the exact broker fact authorizing this retry.
    pub const fn cause(self) -> ConsumerGroupHeartbeatRetryCause {
        self.cause
    }

    /// Returns the earliest absolute moment at which resubmission is allowed.
    pub const fn not_before(self) -> Deadline {
        self.not_before
    }

    /// Returns the original attempt deadline, which is never restarted.
    pub const fn deadline(self) -> Deadline {
        self.deadline
    }
}

impl ConsumerGroupHeartbeatMachine {
    pub(super) fn retry_coordinator_load(
        &mut self,
        attempt: ConsumerGroupHeartbeatAttempt,
        now: Moment,
        failure: ConsumerGroupHeartbeatFailure,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        let kind = self.retry_kind(attempt)?;
        if self.retry_schedule.is_some() {
            return Err(ConsumerGroupHeartbeatErrorKind::CoordinatorLoadRetryPending);
        }
        if failure != ConsumerGroupHeartbeatFailure::Broker(14) {
            return Err(ConsumerGroupHeartbeatErrorKind::FailureNotCoordinatorLoad);
        }
        let _ = self.retry_facts(kind)?;
        let deadline = self
            .deadline
            .ok_or(ConsumerGroupHeartbeatErrorKind::InvariantViolation)?;
        if deadline.is_elapsed_at(now) {
            return Ok(self.fail(attempt, ConsumerGroupHeartbeatFailure::DeadlineElapsed));
        }
        let schedule = retry_schedule(
            attempt,
            kind,
            ConsumerGroupHeartbeatRetryCause::CoordinatorLoad,
            now,
            deadline,
        );
        self.retry_schedule = Some(schedule);
        Ok(ConsumerGroupHeartbeatTransition::one(
            ConsumerGroupHeartbeatEffect::ArmCoordinatorLoadRetry { schedule },
        ))
    }

    pub(super) fn coordinator_load_retry_due(
        &mut self,
        schedule: ConsumerGroupHeartbeatRetrySchedule,
        now: Moment,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        let kind = self.retry_kind(schedule.attempt())?;
        if self.retry_schedule != Some(schedule)
            || kind != schedule.kind()
            || self.deadline != Some(schedule.deadline())
        {
            return Err(ConsumerGroupHeartbeatErrorKind::CoordinatorLoadRetryScheduleMismatch);
        }
        if !schedule.not_before().is_elapsed_at(now) {
            return Err(ConsumerGroupHeartbeatErrorKind::CoordinatorLoadRetryNotDue);
        }
        if schedule.deadline().is_elapsed_at(now) {
            return Ok(self.fail(
                schedule.attempt(),
                ConsumerGroupHeartbeatFailure::DeadlineElapsed,
            ));
        }
        let (member_id, member_epoch, assignment_generation) = self.retry_facts(kind)?;
        self.retry_schedule = None;
        Ok(ConsumerGroupHeartbeatTransition::one(
            ConsumerGroupHeartbeatEffect::Submit {
                group_id: self.group_id,
                attempt: schedule.attempt(),
                kind,
                member_id,
                member_epoch,
                assignment_generation,
                deadline: schedule.deadline(),
            },
        ))
    }

    pub(super) fn retry_kind(
        &self,
        attempt: ConsumerGroupHeartbeatAttempt,
    ) -> Result<ConsumerGroupHeartbeatRequestKind, ConsumerGroupHeartbeatErrorKind> {
        let kind = match self.phase {
            ConsumerGroupHeartbeatPhase::Joining => ConsumerGroupHeartbeatRequestKind::Join,
            ConsumerGroupHeartbeatPhase::Heartbeating => ConsumerGroupHeartbeatRequestKind::Steady,
            ConsumerGroupHeartbeatPhase::Leaving => ConsumerGroupHeartbeatRequestKind::Leave,
            _ => return Err(ConsumerGroupHeartbeatErrorKind::InvalidPhase),
        };
        if self.in_flight != Some(attempt) {
            return Err(ConsumerGroupHeartbeatErrorKind::AttemptMismatch);
        }
        Ok(kind)
    }

    pub(super) fn retry_facts(
        &self,
        kind: ConsumerGroupHeartbeatRequestKind,
    ) -> Result<RetryFacts, ConsumerGroupHeartbeatErrorKind> {
        if kind == ConsumerGroupHeartbeatRequestKind::Join {
            if self.member_epoch.is_some()
                || self.live_assignment.is_some()
                || self.pending_assignment.is_some()
                || self
                    .in_flight
                    .is_none_or(|attempt| attempt.member_epoch().is_some())
            {
                return Err(ConsumerGroupHeartbeatErrorKind::InvariantViolation);
            }
            return Ok((self.member_id, None, None));
        }
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
            None if kind == ConsumerGroupHeartbeatRequestKind::Steady
                && self.pending_assignment.as_ref().is_some_and(|assignment| {
                    assignment.group_id() == self.group_id && assignment.member_id() == member_id
                }) =>
            {
                None
            }
            None if self.pending_assignment.is_none()
                && matches!(
                    kind,
                    ConsumerGroupHeartbeatRequestKind::Steady
                        | ConsumerGroupHeartbeatRequestKind::Leave
                ) =>
            {
                None
            }
            _ => return Err(ConsumerGroupHeartbeatErrorKind::InvariantViolation),
        };
        if self
            .in_flight
            .is_none_or(|attempt| attempt.member_epoch() != Some(member_epoch))
        {
            return Err(ConsumerGroupHeartbeatErrorKind::InvariantViolation);
        }
        Ok((Some(member_id), Some(member_epoch), assignment_generation))
    }
}

pub(super) fn retry_schedule(
    attempt: ConsumerGroupHeartbeatAttempt,
    kind: ConsumerGroupHeartbeatRequestKind,
    cause: ConsumerGroupHeartbeatRetryCause,
    now: Moment,
    deadline: Deadline,
) -> ConsumerGroupHeartbeatRetrySchedule {
    let not_before = now
        .checked_deadline_after(CONSUMER_GROUP_HEARTBEAT_RETRY_BACKOFF_TICKS)
        .map_or(deadline, |backoff| backoff.min(deadline));
    ConsumerGroupHeartbeatRetrySchedule::new(attempt, kind, cause, not_before, deadline)
}

type RetryFacts = (
    Option<crate::MemberId>,
    Option<super::ConsumerGroupMemberEpoch>,
    Option<crate::AssignmentGeneration>,
);
