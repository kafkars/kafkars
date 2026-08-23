//! Explicit epoch-zero recovery after a share member is fenced.

use crate::Moment;

use super::{
    ShareGroupHeartbeatAttempt, ShareGroupHeartbeatEffect, ShareGroupHeartbeatErrorKind,
    ShareGroupHeartbeatFailure, ShareGroupHeartbeatMachine, ShareGroupHeartbeatPhase,
    ShareGroupHeartbeatRequestKind, ShareGroupHeartbeatTransition,
};

impl ShareGroupHeartbeatMachine {
    pub(super) fn recover_fenced_membership(
        &mut self,
        attempt: ShareGroupHeartbeatAttempt,
        now: Moment,
        failure: ShareGroupHeartbeatFailure,
    ) -> Result<ShareGroupHeartbeatTransition, ShareGroupHeartbeatErrorKind> {
        if self.phase != ShareGroupHeartbeatPhase::Heartbeating {
            return Err(ShareGroupHeartbeatErrorKind::InvalidPhase);
        }
        if self.in_flight != Some(attempt) {
            return Err(ShareGroupHeartbeatErrorKind::AttemptMismatch);
        }
        if self.retry_schedule.is_some() {
            return Err(ShareGroupHeartbeatErrorKind::RetryPending);
        }
        if !matches!(failure, ShareGroupHeartbeatFailure::Broker(25 | 110)) {
            return Err(ShareGroupHeartbeatErrorKind::FailureNotRecoverable);
        }
        let original_deadline = self
            .deadline
            .ok_or(ShareGroupHeartbeatErrorKind::InvariantViolation)?;
        if original_deadline.is_elapsed_at(now) {
            return Ok(self.fail(attempt, ShareGroupHeartbeatFailure::DeadlineElapsed));
        }
        let member_epoch = self
            .member_epoch
            .ok_or(ShareGroupHeartbeatErrorKind::InvariantViolation)?;
        if attempt.member_epoch() != Some(member_epoch) {
            return Err(ShareGroupHeartbeatErrorKind::InvariantViolation);
        }
        let deadline = now
            .checked_deadline_after(self.policy.attempt_timeout_ticks())
            .ok_or(ShareGroupHeartbeatErrorKind::DeadlineOverflow)?;
        let (join_attempt, next_sequence) = self.reserve_attempt(None)?;
        let assignment = self.live_assignment.take();
        self.phase = ShareGroupHeartbeatPhase::Joining;
        self.next_sequence = next_sequence;
        self.in_flight = Some(join_attempt);
        self.deadline = Some(deadline);
        self.retry_schedule = None;
        self.member_epoch = None;
        self.schedule = None;
        let submit = self.submit_effect(
            join_attempt,
            ShareGroupHeartbeatRequestKind::Join,
            None,
            None,
            deadline,
        );
        Ok(match assignment {
            Some(assignment) => ShareGroupHeartbeatTransition::two(
                ShareGroupHeartbeatEffect::Revoke { assignment },
                submit,
            ),
            None => ShareGroupHeartbeatTransition::one(submit),
        })
    }
}
