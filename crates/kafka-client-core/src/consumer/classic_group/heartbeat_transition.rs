//! Assignment-fenced heartbeat cadence and conservative loss transitions.

use crate::Moment;

use super::{
    ClassicGroupEffect, ClassicGroupErrorKind, ClassicGroupMachine, ClassicGroupPhase,
    ClassicGroupTransition, ClassicHeartbeatAttempt,
    heartbeat_state::{ClassicHeartbeatDue, ClassicHeartbeatSuccess},
};

impl ClassicGroupMachine {
    pub(super) fn heartbeat_due(
        &mut self,
        attempt: ClassicHeartbeatAttempt,
        now: Moment,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        self.validate_heartbeat_assignment(attempt)?;
        match self.heartbeat.due(attempt, now)? {
            ClassicHeartbeatDue::Submit(deadline) => {
                let assignment = self
                    .live_assignment()
                    .ok_or(ClassicGroupErrorKind::InvariantViolation)?;
                let classic_generation = self
                    .live_generation()
                    .ok_or(ClassicGroupErrorKind::InvariantViolation)?;
                Ok(ClassicGroupTransition::one(
                    ClassicGroupEffect::SubmitHeartbeat {
                        group_id: self.group_id,
                        attempt,
                        member_id: assignment.member_id(),
                        classic_generation,
                        deadline,
                    },
                ))
            }
            ClassicHeartbeatDue::Lost => self.revoke_stable_assignment(),
        }
    }

    pub(super) fn heartbeat_succeeded(
        &mut self,
        attempt: ClassicHeartbeatAttempt,
        now: Moment,
        throttle_ticks: u64,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        self.validate_heartbeat_assignment(attempt)?;
        let session_timeout_ticks = self.timing().session_timeout_ticks();
        match self
            .heartbeat
            .succeeded(attempt, now, throttle_ticks, session_timeout_ticks)?
        {
            ClassicHeartbeatSuccess::Schedule(schedule) => Ok(ClassicGroupTransition::one(
                ClassicGroupEffect::ArmHeartbeat { schedule },
            )),
            ClassicHeartbeatSuccess::Lost => self.revoke_stable_assignment(),
        }
    }

    pub(super) fn heartbeat_failed(
        &mut self,
        attempt: ClassicHeartbeatAttempt,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        self.validate_heartbeat_assignment(attempt)?;
        self.heartbeat.failed(attempt)?;
        self.revoke_stable_assignment()
    }

    pub(super) fn heartbeat_deadline_elapsed(
        &mut self,
        attempt: ClassicHeartbeatAttempt,
        now: Moment,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        self.validate_heartbeat_assignment(attempt)?;
        self.heartbeat.deadline_elapsed(attempt, now)?;
        self.revoke_stable_assignment()
    }

    fn validate_heartbeat_assignment(
        &self,
        attempt: ClassicHeartbeatAttempt,
    ) -> Result<(), ClassicGroupErrorKind> {
        if self.phase != ClassicGroupPhase::Stable {
            return Err(ClassicGroupErrorKind::InvalidPhase);
        }
        if self.active_cycle != Some(attempt.cycle()) {
            return Err(ClassicGroupErrorKind::HeartbeatMismatch);
        }
        let assignment = self
            .live_assignment()
            .ok_or(ClassicGroupErrorKind::InvariantViolation)?;
        if assignment.assignment_generation() != attempt.assignment_generation() {
            return Err(ClassicGroupErrorKind::HeartbeatMismatch);
        }
        if self.live_generation().is_none() {
            return Err(ClassicGroupErrorKind::InvariantViolation);
        }
        Ok(())
    }
}
