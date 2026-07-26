//! Private Dormant, Waiting, and `InFlight` classic heartbeat ownership.

use crate::{Deadline, Moment};

use super::{
    ClassicGroupErrorKind, ClassicHeartbeatAttempt, ClassicHeartbeatPolicy,
    ClassicHeartbeatSchedule, MembershipCycle,
};

#[derive(Debug, Eq, PartialEq)]
pub(super) struct ClassicHeartbeatState {
    policy: ClassicHeartbeatPolicy,
    phase: ClassicHeartbeatPhase,
}

#[derive(Debug, Eq, PartialEq)]
enum ClassicHeartbeatPhase {
    Dormant,
    Waiting(ClassicHeartbeatSchedule),
    InFlight {
        attempt: ClassicHeartbeatAttempt,
        deadline: Deadline,
        sent_at: Moment,
    },
}

pub(super) enum ClassicHeartbeatDue {
    Submit(Deadline),
    Lost,
}

pub(super) enum ClassicHeartbeatSuccess {
    Schedule(ClassicHeartbeatSchedule),
    Lost,
}

impl ClassicHeartbeatState {
    pub(super) const fn new(policy: ClassicHeartbeatPolicy) -> Self {
        Self {
            policy,
            phase: ClassicHeartbeatPhase::Dormant,
        }
    }

    pub(super) fn prepare_activation(
        &self,
        cycle: MembershipCycle,
        assignment_generation: crate::AssignmentGeneration,
        now: Moment,
        liveness_deadline: Deadline,
    ) -> Result<Option<ClassicHeartbeatSchedule>, ClassicGroupErrorKind> {
        if !matches!(self.phase, ClassicHeartbeatPhase::Dormant) {
            return Err(ClassicGroupErrorKind::InvariantViolation);
        }
        if liveness_deadline.is_elapsed_at(now) {
            return Ok(None);
        }
        Ok(Some(ClassicHeartbeatSchedule::new(
            ClassicHeartbeatAttempt::first(cycle, assignment_generation),
            Deadline::from_tick(now.tick()),
            liveness_deadline,
        )))
    }

    pub(super) fn activate(&mut self, schedule: ClassicHeartbeatSchedule) {
        self.phase = ClassicHeartbeatPhase::Waiting(schedule);
    }

    pub(super) fn due(
        &mut self,
        attempt: ClassicHeartbeatAttempt,
        now: Moment,
    ) -> Result<ClassicHeartbeatDue, ClassicGroupErrorKind> {
        let ClassicHeartbeatPhase::Waiting(schedule) = self.phase else {
            return Err(ClassicGroupErrorKind::InvalidPhase);
        };
        if schedule.attempt() != attempt {
            return Err(ClassicGroupErrorKind::HeartbeatMismatch);
        }
        if schedule.liveness_deadline().is_elapsed_at(now) {
            self.disarm();
            return Ok(ClassicHeartbeatDue::Lost);
        }
        if !schedule.due().is_elapsed_at(now) {
            return Err(ClassicGroupErrorKind::DeadlineNotElapsed);
        }
        let deadline = now
            .checked_deadline_after(self.policy.attempt_timeout_ticks())
            .map_or(schedule.liveness_deadline(), |attempt_deadline| {
                attempt_deadline.min(schedule.liveness_deadline())
            });
        self.phase = ClassicHeartbeatPhase::InFlight {
            attempt,
            deadline,
            sent_at: now,
        };
        Ok(ClassicHeartbeatDue::Submit(deadline))
    }

    pub(super) fn succeeded(
        &mut self,
        attempt: ClassicHeartbeatAttempt,
        now: Moment,
        throttle_ticks: u64,
        session_timeout_ticks: u64,
    ) -> Result<ClassicHeartbeatSuccess, ClassicGroupErrorKind> {
        let (deadline, sent_at) = self.require_inflight(attempt)?;
        let Some(liveness) = sent_at.checked_deadline_after(session_timeout_ticks) else {
            self.disarm();
            return Ok(ClassicHeartbeatSuccess::Lost);
        };
        if deadline.is_elapsed_at(now) || liveness.is_elapsed_at(now) {
            self.disarm();
            return Ok(ClassicHeartbeatSuccess::Lost);
        }
        let Some(next) = attempt.checked_next() else {
            self.disarm();
            return Ok(ClassicHeartbeatSuccess::Lost);
        };
        let delay = self.policy.interval_ticks().max(throttle_ticks);
        let Some(due) = now.checked_deadline_after(delay) else {
            self.disarm();
            return Ok(ClassicHeartbeatSuccess::Lost);
        };
        if liveness.tick() <= due.tick() {
            self.disarm();
            return Ok(ClassicHeartbeatSuccess::Lost);
        }
        let schedule = ClassicHeartbeatSchedule::new(next, due, liveness);
        self.phase = ClassicHeartbeatPhase::Waiting(schedule);
        Ok(ClassicHeartbeatSuccess::Schedule(schedule))
    }

    pub(super) fn failed(
        &mut self,
        attempt: ClassicHeartbeatAttempt,
    ) -> Result<(), ClassicGroupErrorKind> {
        self.require_inflight(attempt)?;
        self.disarm();
        Ok(())
    }

    pub(super) fn attempt_deadline_is_elapsed(
        &self,
        attempt: ClassicHeartbeatAttempt,
        now: Moment,
    ) -> Result<bool, ClassicGroupErrorKind> {
        let (deadline, _sent_at) = self.require_inflight(attempt)?;
        Ok(deadline.is_elapsed_at(now))
    }

    pub(super) fn deadline_elapsed(
        &mut self,
        attempt: ClassicHeartbeatAttempt,
        now: Moment,
    ) -> Result<(), ClassicGroupErrorKind> {
        let (deadline, _sent_at) = self.require_inflight(attempt)?;
        if !deadline.is_elapsed_at(now) {
            return Err(ClassicGroupErrorKind::DeadlineNotElapsed);
        }
        self.disarm();
        Ok(())
    }

    pub(super) fn disarm(&mut self) {
        self.phase = ClassicHeartbeatPhase::Dormant;
    }

    fn require_inflight(
        &self,
        attempt: ClassicHeartbeatAttempt,
    ) -> Result<(Deadline, Moment), ClassicGroupErrorKind> {
        match self.phase {
            ClassicHeartbeatPhase::InFlight {
                attempt: active,
                deadline,
                sent_at,
            } if active == attempt => Ok((deadline, sent_at)),
            ClassicHeartbeatPhase::InFlight { .. } => Err(ClassicGroupErrorKind::HeartbeatMismatch),
            ClassicHeartbeatPhase::Dormant | ClassicHeartbeatPhase::Waiting(_) => {
                Err(ClassicGroupErrorKind::InvalidPhase)
            }
        }
    }
}
