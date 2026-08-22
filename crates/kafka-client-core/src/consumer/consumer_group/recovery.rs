//! Explicit epoch-zero recovery after a steady KIP-848 member is fenced.

use crate::Moment;

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatErrorKind,
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatPhase,
    ConsumerGroupHeartbeatRequestKind, ConsumerGroupHeartbeatTransition,
};

impl ConsumerGroupHeartbeatMachine {
    pub(super) fn recover_fenced_membership(
        &mut self,
        attempt: ConsumerGroupHeartbeatAttempt,
        now: Moment,
        failure: ConsumerGroupHeartbeatFailure,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        if self.phase != ConsumerGroupHeartbeatPhase::Heartbeating {
            return Err(ConsumerGroupHeartbeatErrorKind::InvalidPhase);
        }
        if self.in_flight != Some(attempt) {
            return Err(ConsumerGroupHeartbeatErrorKind::AttemptMismatch);
        }
        if self.retry_schedule.is_some() {
            return Err(ConsumerGroupHeartbeatErrorKind::CoordinatorLoadRetryPending);
        }
        if !matches!(failure, ConsumerGroupHeartbeatFailure::Broker(25 | 110)) {
            return Err(ConsumerGroupHeartbeatErrorKind::FailureNotRecoverable);
        }
        let original_deadline = self
            .deadline
            .ok_or(ConsumerGroupHeartbeatErrorKind::InvariantViolation)?;
        if original_deadline.is_elapsed_at(now) {
            return Ok(self.fail(attempt, ConsumerGroupHeartbeatFailure::DeadlineElapsed));
        }
        let member_id = self
            .member_id
            .ok_or(ConsumerGroupHeartbeatErrorKind::InvariantViolation)?;
        let member_epoch = self
            .member_epoch
            .ok_or(ConsumerGroupHeartbeatErrorKind::InvariantViolation)?;
        let assignment_is_valid = self.live_assignment.as_ref().is_some_and(|assignment| {
            assignment.group_id() == self.group_id && assignment.member_id() == member_id
        });
        let empty_ack_is_valid = self.live_assignment.is_none()
            && self.pending_assignment.as_ref().is_some_and(|assignment| {
                assignment.group_id() == self.group_id && assignment.member_id() == member_id
            });
        let awaiting_assignment_is_valid =
            self.live_assignment.is_none() && self.pending_assignment.is_none();
        if attempt.member_epoch() != Some(member_epoch)
            || !(assignment_is_valid || empty_ack_is_valid || awaiting_assignment_is_valid)
        {
            return Err(ConsumerGroupHeartbeatErrorKind::InvariantViolation);
        }

        let deadline = now
            .checked_deadline_after(self.policy.attempt_timeout_ticks())
            .ok_or(ConsumerGroupHeartbeatErrorKind::DeadlineOverflow)?;
        let (join_attempt, next_sequence) = self.reserve_attempt(None)?;
        let assignment = self.live_assignment.take();
        drop(self.pending_assignment.take());
        self.phase = ConsumerGroupHeartbeatPhase::Joining;
        self.next_sequence = next_sequence;
        self.in_flight = Some(join_attempt);
        self.deadline = Some(deadline);
        self.retry_schedule = None;
        self.member_epoch = None;
        self.schedule = None;

        let submit = ConsumerGroupHeartbeatEffect::Submit {
            group_id: self.group_id,
            attempt: join_attempt,
            kind: ConsumerGroupHeartbeatRequestKind::Join,
            member_id: Some(member_id),
            member_epoch: None,
            assignment_generation: None,
            deadline,
        };
        Ok(match assignment {
            Some(assignment) => ConsumerGroupHeartbeatTransition::two(
                ConsumerGroupHeartbeatEffect::Revoke { assignment },
                submit,
            ),
            None => ConsumerGroupHeartbeatTransition::one(submit),
        })
    }
}
