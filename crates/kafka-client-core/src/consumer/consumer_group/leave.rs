//! Explicit epoch-minus-one leave transitions for one KIP-848 member.

use crate::{Deadline, Moment};

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatErrorKind,
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatPhase,
    ConsumerGroupHeartbeatRequestKind, ConsumerGroupHeartbeatTransition,
};

impl ConsumerGroupHeartbeatMachine {
    pub(super) fn close(
        &mut self,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        if self.phase == ConsumerGroupHeartbeatPhase::Closed {
            return Err(ConsumerGroupHeartbeatErrorKind::Closed);
        }
        let assignment = self.live_assignment.take();
        self.phase = ConsumerGroupHeartbeatPhase::Closed;
        self.next_sequence = None;
        self.next_assignment_generation = None;
        self.clear_active();
        Ok(
            assignment.map_or_else(ConsumerGroupHeartbeatTransition::none, |assignment| {
                ConsumerGroupHeartbeatTransition::one(ConsumerGroupHeartbeatEffect::Revoke {
                    assignment,
                })
            }),
        )
    }

    pub(super) fn begin_leave(
        &mut self,
        now: Moment,
        deadline: Deadline,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        if self.phase == ConsumerGroupHeartbeatPhase::Closed {
            return Err(ConsumerGroupHeartbeatErrorKind::Closed);
        }
        if self.phase == ConsumerGroupHeartbeatPhase::Dormant
            || self.phase == ConsumerGroupHeartbeatPhase::Fatal
        {
            self.phase = ConsumerGroupHeartbeatPhase::Closed;
            return Ok(ConsumerGroupHeartbeatTransition::none());
        }
        if !matches!(
            self.phase,
            ConsumerGroupHeartbeatPhase::Stable | ConsumerGroupHeartbeatPhase::AwaitingAssignment
        ) {
            return Err(ConsumerGroupHeartbeatErrorKind::InvalidPhase);
        }
        if deadline.is_elapsed_at(now) {
            return Err(ConsumerGroupHeartbeatErrorKind::DeadlineElapsed);
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
        let (attempt, next_sequence) = self.reserve_attempt(Some(member_epoch))?;
        self.phase = ConsumerGroupHeartbeatPhase::Leaving;
        self.next_sequence = next_sequence;
        self.schedule = None;
        self.in_flight = Some(attempt);
        self.deadline = Some(deadline);
        self.retry_schedule = None;
        Ok(ConsumerGroupHeartbeatTransition::one(
            ConsumerGroupHeartbeatEffect::Submit {
                group_id: self.group_id,
                attempt,
                kind: ConsumerGroupHeartbeatRequestKind::Leave,
                member_id: Some(member_id),
                member_epoch: Some(member_epoch),
                assignment_generation,
                deadline,
            },
        ))
    }

    pub(super) fn leave_succeeded(
        &mut self,
        attempt: ConsumerGroupHeartbeatAttempt,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        if self.phase != ConsumerGroupHeartbeatPhase::Leaving {
            return Err(ConsumerGroupHeartbeatErrorKind::InvalidPhase);
        }
        if self.in_flight != Some(attempt) {
            return Err(ConsumerGroupHeartbeatErrorKind::AttemptMismatch);
        }
        if self.retry_schedule.is_some() {
            return Err(ConsumerGroupHeartbeatErrorKind::CoordinatorLoadRetryPending);
        }
        let assignment = self.live_assignment.take();
        self.phase = ConsumerGroupHeartbeatPhase::Closed;
        self.clear_active();
        Ok(
            assignment.map_or_else(ConsumerGroupHeartbeatTransition::none, |assignment| {
                ConsumerGroupHeartbeatTransition::one(ConsumerGroupHeartbeatEffect::Revoke {
                    assignment,
                })
            }),
        )
    }

    pub(super) fn leave_failed(
        &mut self,
        attempt: ConsumerGroupHeartbeatAttempt,
        failure: ConsumerGroupHeartbeatFailure,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        if self.phase != ConsumerGroupHeartbeatPhase::Leaving {
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
