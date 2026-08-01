//! Same-attempt KIP-848 retry after `COORDINATOR_LOAD_IN_PROGRESS`.

use crate::{Deadline, Moment};

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatErrorKind,
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatPhase,
    ConsumerGroupHeartbeatRequestKind, ConsumerGroupHeartbeatTransition,
};

const CONSUMER_GROUP_COORDINATOR_LOAD_BACKOFF_TICKS: u64 = 100_000_000;

/// Exact future fence for retrying one unchanged coordinator request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerGroupHeartbeatRetrySchedule {
    attempt: ConsumerGroupHeartbeatAttempt,
    kind: ConsumerGroupHeartbeatRequestKind,
    not_before: Deadline,
    deadline: Deadline,
}

impl ConsumerGroupHeartbeatRetrySchedule {
    const fn new(
        attempt: ConsumerGroupHeartbeatAttempt,
        kind: ConsumerGroupHeartbeatRequestKind,
        not_before: Deadline,
        deadline: Deadline,
    ) -> Self {
        Self {
            attempt,
            kind,
            not_before,
            deadline,
        }
    }

    /// Returns the exact unchanged request identity.
    pub const fn attempt(self) -> ConsumerGroupHeartbeatAttempt {
        self.attempt
    }

    /// Returns the unchanged Join, Steady, or Leave request shape.
    pub const fn kind(self) -> ConsumerGroupHeartbeatRequestKind {
        self.kind
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
        let not_before = now
            .checked_deadline_after(CONSUMER_GROUP_COORDINATOR_LOAD_BACKOFF_TICKS)
            .map_or(deadline, |backoff| backoff.min(deadline));
        let schedule =
            ConsumerGroupHeartbeatRetrySchedule::new(attempt, kind, not_before, deadline);
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

    fn retry_kind(
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

    fn retry_facts(
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

type RetryFacts = (
    Option<crate::MemberId>,
    Option<super::ConsumerGroupMemberEpoch>,
    Option<crate::AssignmentGeneration>,
);
