//! Sole basic state mutation paths for share heartbeat ownership.

use crate::{Deadline, Moment};

use super::{
    ShareGroupHeartbeatAttempt, ShareGroupHeartbeatEffect, ShareGroupHeartbeatErrorKind,
    ShareGroupHeartbeatFailure, ShareGroupHeartbeatFatal, ShareGroupHeartbeatMachine,
    ShareGroupHeartbeatPhase, ShareGroupHeartbeatRequestKind, ShareGroupHeartbeatSchedule,
    ShareGroupHeartbeatSequence, ShareGroupHeartbeatTransition, ShareGroupMemberEpoch,
};

impl ShareGroupHeartbeatMachine {
    pub(super) fn begin(
        &mut self,
        now: Moment,
        deadline: Deadline,
    ) -> Result<ShareGroupHeartbeatTransition, ShareGroupHeartbeatErrorKind> {
        if self.phase == ShareGroupHeartbeatPhase::Closed {
            return Err(ShareGroupHeartbeatErrorKind::Closed);
        }
        if self.phase != ShareGroupHeartbeatPhase::Dormant {
            return Err(ShareGroupHeartbeatErrorKind::InvalidPhase);
        }
        if deadline.is_elapsed_at(now) {
            return Err(ShareGroupHeartbeatErrorKind::DeadlineElapsed);
        }
        if self.member_epoch.is_some() || self.live_assignment.is_some() {
            return Err(ShareGroupHeartbeatErrorKind::InvariantViolation);
        }
        let (attempt, next_sequence) = self.reserve_attempt(None)?;
        self.phase = ShareGroupHeartbeatPhase::Joining;
        self.next_sequence = next_sequence;
        self.in_flight = Some(attempt);
        self.deadline = Some(deadline);
        self.retry_schedule = None;
        Ok(ShareGroupHeartbeatTransition::one(self.submit_effect(
            attempt,
            ShareGroupHeartbeatRequestKind::Join,
            None,
            None,
            deadline,
        )))
    }

    pub(super) fn heartbeat_due(
        &mut self,
        schedule: ShareGroupHeartbeatSchedule,
        now: Moment,
    ) -> Result<ShareGroupHeartbeatTransition, ShareGroupHeartbeatErrorKind> {
        if !matches!(
            self.phase,
            ShareGroupHeartbeatPhase::Stable | ShareGroupHeartbeatPhase::AwaitingAssignment
        ) {
            return Err(ShareGroupHeartbeatErrorKind::InvalidPhase);
        }
        if self.schedule != Some(schedule) {
            return Err(ShareGroupHeartbeatErrorKind::ScheduleMismatch);
        }
        if !schedule.deadline().is_elapsed_at(now) {
            return Err(ShareGroupHeartbeatErrorKind::ScheduleNotDue);
        }
        let member_epoch = self
            .member_epoch
            .ok_or(ShareGroupHeartbeatErrorKind::InvariantViolation)?;
        let assignment_generation = self.current_assignment_generation()?;
        if schedule.assignment_generation() != assignment_generation
            || schedule.attempt().member_epoch() != Some(member_epoch)
        {
            return Err(ShareGroupHeartbeatErrorKind::ScheduleMismatch);
        }
        let deadline = now
            .checked_deadline_after(self.policy.attempt_timeout_ticks())
            .ok_or(ShareGroupHeartbeatErrorKind::DeadlineOverflow)?;
        let attempt = schedule.attempt();
        self.phase = ShareGroupHeartbeatPhase::Heartbeating;
        self.schedule = None;
        self.in_flight = Some(attempt);
        self.deadline = Some(deadline);
        self.retry_schedule = None;
        Ok(ShareGroupHeartbeatTransition::one(self.submit_effect(
            attempt,
            ShareGroupHeartbeatRequestKind::Steady,
            Some(member_epoch),
            assignment_generation,
            deadline,
        )))
    }

    pub(super) fn heartbeat_failed(
        &mut self,
        attempt: ShareGroupHeartbeatAttempt,
        failure: ShareGroupHeartbeatFailure,
    ) -> Result<ShareGroupHeartbeatTransition, ShareGroupHeartbeatErrorKind> {
        if !matches!(
            self.phase,
            ShareGroupHeartbeatPhase::Joining | ShareGroupHeartbeatPhase::Heartbeating
        ) {
            return Err(ShareGroupHeartbeatErrorKind::InvalidPhase);
        }
        self.validate_attempt(attempt)?;
        Ok(self.fail(attempt, failure))
    }

    pub(super) fn validate_attempt(
        &self,
        attempt: ShareGroupHeartbeatAttempt,
    ) -> Result<(), ShareGroupHeartbeatErrorKind> {
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
        Ok(())
    }

    pub(super) fn validate_in_flight(
        &self,
        attempt: ShareGroupHeartbeatAttempt,
        now: Moment,
    ) -> Result<(), ShareGroupHeartbeatErrorKind> {
        self.validate_attempt(attempt)?;
        if self
            .deadline
            .is_none_or(|deadline| deadline.is_elapsed_at(now))
        {
            return Err(ShareGroupHeartbeatErrorKind::DeadlineElapsed);
        }
        Ok(())
    }

    pub(super) fn reserve_attempt(
        &self,
        member_epoch: Option<ShareGroupMemberEpoch>,
    ) -> Result<
        (
            ShareGroupHeartbeatAttempt,
            Option<ShareGroupHeartbeatSequence>,
        ),
        ShareGroupHeartbeatErrorKind,
    > {
        let sequence = self
            .next_sequence
            .ok_or(ShareGroupHeartbeatErrorKind::AttemptExhausted)?;
        Ok((
            ShareGroupHeartbeatAttempt::new(sequence, member_epoch),
            sequence.checked_next(),
        ))
    }

    pub(super) fn current_assignment_generation(
        &self,
    ) -> Result<Option<crate::AssignmentGeneration>, ShareGroupHeartbeatErrorKind> {
        match (&self.live_assignment, self.phase) {
            (Some(assignment), ShareGroupHeartbeatPhase::Stable) => {
                Ok(Some(assignment.assignment_generation()))
            }
            (None, ShareGroupHeartbeatPhase::AwaitingAssignment) => Ok(None),
            (
                Some(assignment),
                ShareGroupHeartbeatPhase::Heartbeating | ShareGroupHeartbeatPhase::Leaving,
            ) if assignment.group_id() == self.group_id
                && assignment.member_id() == self.member_id =>
            {
                Ok(Some(assignment.assignment_generation()))
            }
            (None, ShareGroupHeartbeatPhase::Heartbeating | ShareGroupHeartbeatPhase::Leaving) => {
                Ok(None)
            }
            _ => Err(ShareGroupHeartbeatErrorKind::InvariantViolation),
        }
    }

    pub(super) const fn submit_effect(
        &self,
        attempt: ShareGroupHeartbeatAttempt,
        kind: ShareGroupHeartbeatRequestKind,
        member_epoch: Option<ShareGroupMemberEpoch>,
        assignment_generation: Option<crate::AssignmentGeneration>,
        deadline: Deadline,
    ) -> ShareGroupHeartbeatEffect {
        ShareGroupHeartbeatEffect::Submit {
            group_id: self.group_id,
            member_id: self.member_id,
            attempt,
            kind,
            member_epoch,
            assignment_generation,
            deadline,
        }
    }

    pub(super) fn fail(
        &mut self,
        attempt: ShareGroupHeartbeatAttempt,
        failure: ShareGroupHeartbeatFailure,
    ) -> ShareGroupHeartbeatTransition {
        let assignment = self.live_assignment.take();
        let fatal = ShareGroupHeartbeatFatal::new(attempt, failure);
        self.phase = ShareGroupHeartbeatPhase::Fatal;
        self.clear_active();
        self.fatal = Some(fatal);
        match assignment {
            Some(assignment) => ShareGroupHeartbeatTransition::two(
                ShareGroupHeartbeatEffect::Revoke { assignment },
                ShareGroupHeartbeatEffect::Fatal { fatal },
            ),
            None => ShareGroupHeartbeatTransition::one(ShareGroupHeartbeatEffect::Fatal { fatal }),
        }
    }

    pub(super) fn clear_active(&mut self) {
        self.in_flight = None;
        self.deadline = None;
        self.retry_schedule = None;
        self.member_epoch = None;
        self.schedule = None;
    }
}
