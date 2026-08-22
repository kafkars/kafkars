//! Sole state mutation paths for KIP-848 heartbeat and assignment ownership.

use crate::{Deadline, Moment};

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatErrorKind,
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatPhase,
    ConsumerGroupHeartbeatRequestKind, ConsumerGroupHeartbeatSchedule,
    ConsumerGroupHeartbeatTransition,
};

impl ConsumerGroupHeartbeatMachine {
    pub(super) fn begin(
        &mut self,
        now: Moment,
        deadline: Deadline,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        if self.phase == ConsumerGroupHeartbeatPhase::Closed {
            return Err(ConsumerGroupHeartbeatErrorKind::Closed);
        }
        if self.phase != ConsumerGroupHeartbeatPhase::Dormant {
            return Err(ConsumerGroupHeartbeatErrorKind::InvalidPhase);
        }
        if deadline.is_elapsed_at(now) {
            return Err(ConsumerGroupHeartbeatErrorKind::DeadlineElapsed);
        }
        if self.member_id.is_some() || self.member_epoch.is_some() || self.live_assignment.is_some()
        {
            return Err(ConsumerGroupHeartbeatErrorKind::InvariantViolation);
        }
        let (attempt, next_sequence) = self.reserve_attempt(None)?;
        self.phase = ConsumerGroupHeartbeatPhase::Joining;
        self.next_sequence = next_sequence;
        self.in_flight = Some(attempt);
        self.deadline = Some(deadline);
        self.retry_schedule = None;
        Ok(ConsumerGroupHeartbeatTransition::one(
            ConsumerGroupHeartbeatEffect::Submit {
                group_id: self.group_id,
                attempt,
                kind: ConsumerGroupHeartbeatRequestKind::Join,
                member_id: None,
                member_epoch: None,
                assignment_generation: None,
                deadline,
            },
        ))
    }

    pub(super) fn heartbeat_due(
        &mut self,
        schedule: ConsumerGroupHeartbeatSchedule,
        now: Moment,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        if !matches!(
            self.phase,
            ConsumerGroupHeartbeatPhase::Stable | ConsumerGroupHeartbeatPhase::AwaitingAssignment
        ) {
            return Err(ConsumerGroupHeartbeatErrorKind::InvalidPhase);
        }
        if self.schedule != Some(schedule) {
            return Err(ConsumerGroupHeartbeatErrorKind::ScheduleMismatch);
        }
        if !schedule.deadline().is_elapsed_at(now) {
            return Err(ConsumerGroupHeartbeatErrorKind::ScheduleNotDue);
        }
        let member_id = self
            .member_id
            .ok_or(ConsumerGroupHeartbeatErrorKind::InvariantViolation)?;
        let member_epoch = self
            .member_epoch
            .ok_or(ConsumerGroupHeartbeatErrorKind::InvariantViolation)?;
        let assignment_generation = match self.live_assignment.as_ref() {
            Some(assignment) if self.phase == ConsumerGroupHeartbeatPhase::Stable => {
                Some(assignment.assignment_generation())
            }
            None if self.phase == ConsumerGroupHeartbeatPhase::AwaitingAssignment
                && self.pending_assignment.is_none() =>
            {
                None
            }
            _ => return Err(ConsumerGroupHeartbeatErrorKind::InvariantViolation),
        };
        if assignment_generation != schedule.assignment_generation() {
            return Err(ConsumerGroupHeartbeatErrorKind::ScheduleMismatch);
        }
        let deadline = now
            .checked_deadline_after(self.policy.attempt_timeout_ticks())
            .ok_or(ConsumerGroupHeartbeatErrorKind::DeadlineOverflow)?;
        let attempt = schedule.attempt();
        if attempt.member_epoch() != Some(member_epoch) {
            return Err(ConsumerGroupHeartbeatErrorKind::ScheduleMismatch);
        }
        self.phase = ConsumerGroupHeartbeatPhase::Heartbeating;
        self.schedule = None;
        self.in_flight = Some(attempt);
        self.deadline = Some(deadline);
        self.retry_schedule = None;
        Ok(ConsumerGroupHeartbeatTransition::one(
            ConsumerGroupHeartbeatEffect::Submit {
                group_id: self.group_id,
                attempt,
                kind: ConsumerGroupHeartbeatRequestKind::Steady,
                member_id: Some(member_id),
                member_epoch: Some(member_epoch),
                assignment_generation: self
                    .live_assignment
                    .as_ref()
                    .map(crate::LiveGroupAssignment::assignment_generation),
                deadline,
            },
        ))
    }

    pub(super) fn heartbeat_failed(
        &mut self,
        attempt: ConsumerGroupHeartbeatAttempt,
        failure: ConsumerGroupHeartbeatFailure,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        if !matches!(
            self.phase,
            ConsumerGroupHeartbeatPhase::Joining | ConsumerGroupHeartbeatPhase::Heartbeating
        ) {
            return Err(ConsumerGroupHeartbeatErrorKind::InvalidPhase);
        }
        if self.in_flight != Some(attempt) {
            return Err(ConsumerGroupHeartbeatErrorKind::AttemptMismatch);
        }
        if self.retry_schedule.is_some() {
            return Err(ConsumerGroupHeartbeatErrorKind::CoordinatorLoadRetryPending);
        }
        Ok(self.fail(attempt, failure))
    }
}
