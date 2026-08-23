//! Explicit epoch-minus-one leave transitions for one share member.

use crate::{Deadline, Moment};

use super::{
    ShareGroupHeartbeatAttempt, ShareGroupHeartbeatEffect, ShareGroupHeartbeatErrorKind,
    ShareGroupHeartbeatFailure, ShareGroupHeartbeatMachine, ShareGroupHeartbeatPhase,
    ShareGroupHeartbeatRequestKind, ShareGroupHeartbeatTransition,
};

impl ShareGroupHeartbeatMachine {
    pub(super) fn close(
        &mut self,
    ) -> Result<ShareGroupHeartbeatTransition, ShareGroupHeartbeatErrorKind> {
        if self.phase == ShareGroupHeartbeatPhase::Closed {
            return Err(ShareGroupHeartbeatErrorKind::Closed);
        }
        let assignment = self.live_assignment.take();
        self.phase = ShareGroupHeartbeatPhase::Closed;
        self.next_sequence = None;
        self.next_assignment_generation = None;
        self.clear_active();
        Ok(
            assignment.map_or_else(ShareGroupHeartbeatTransition::none, |assignment| {
                ShareGroupHeartbeatTransition::one(ShareGroupHeartbeatEffect::Revoke { assignment })
            }),
        )
    }

    pub(super) fn begin_leave(
        &mut self,
        now: Moment,
        deadline: Deadline,
    ) -> Result<ShareGroupHeartbeatTransition, ShareGroupHeartbeatErrorKind> {
        if self.phase == ShareGroupHeartbeatPhase::Closed {
            return Err(ShareGroupHeartbeatErrorKind::Closed);
        }
        if matches!(
            self.phase,
            ShareGroupHeartbeatPhase::Dormant | ShareGroupHeartbeatPhase::Fatal
        ) {
            self.phase = ShareGroupHeartbeatPhase::Closed;
            return Ok(ShareGroupHeartbeatTransition::none());
        }
        if !matches!(
            self.phase,
            ShareGroupHeartbeatPhase::Stable | ShareGroupHeartbeatPhase::AwaitingAssignment
        ) {
            return Err(ShareGroupHeartbeatErrorKind::InvalidPhase);
        }
        if deadline.is_elapsed_at(now) {
            return Err(ShareGroupHeartbeatErrorKind::DeadlineElapsed);
        }
        let member_epoch = self
            .member_epoch
            .ok_or(ShareGroupHeartbeatErrorKind::InvariantViolation)?;
        let assignment_generation = self.current_assignment_generation()?;
        let (attempt, next_sequence) = self.reserve_attempt(Some(member_epoch))?;
        self.phase = ShareGroupHeartbeatPhase::Leaving;
        self.next_sequence = next_sequence;
        self.schedule = None;
        self.in_flight = Some(attempt);
        self.deadline = Some(deadline);
        self.retry_schedule = None;
        Ok(ShareGroupHeartbeatTransition::one(self.submit_effect(
            attempt,
            ShareGroupHeartbeatRequestKind::Leave,
            Some(member_epoch),
            assignment_generation,
            deadline,
        )))
    }

    pub(super) fn leave_succeeded(
        &mut self,
        attempt: ShareGroupHeartbeatAttempt,
    ) -> Result<ShareGroupHeartbeatTransition, ShareGroupHeartbeatErrorKind> {
        if self.phase != ShareGroupHeartbeatPhase::Leaving {
            return Err(ShareGroupHeartbeatErrorKind::InvalidPhase);
        }
        self.validate_attempt(attempt)?;
        let assignment = self.live_assignment.take();
        self.phase = ShareGroupHeartbeatPhase::Closed;
        self.clear_active();
        Ok(
            assignment.map_or_else(ShareGroupHeartbeatTransition::none, |assignment| {
                ShareGroupHeartbeatTransition::one(ShareGroupHeartbeatEffect::Revoke { assignment })
            }),
        )
    }

    pub(super) fn leave_failed(
        &mut self,
        attempt: ShareGroupHeartbeatAttempt,
        failure: ShareGroupHeartbeatFailure,
    ) -> Result<ShareGroupHeartbeatTransition, ShareGroupHeartbeatErrorKind> {
        if self.phase != ShareGroupHeartbeatPhase::Leaving {
            return Err(ShareGroupHeartbeatErrorKind::InvalidPhase);
        }
        self.validate_attempt(attempt)?;
        Ok(self.fail(attempt, failure))
    }
}
