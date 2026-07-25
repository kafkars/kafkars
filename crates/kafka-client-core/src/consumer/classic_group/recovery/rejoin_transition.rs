//! Sole owner of pending classic rejoin and fatal-state transitions.

use crate::Moment;

use super::{
    ClassicGroupEffect, ClassicGroupErrorKind, ClassicGroupFatal, ClassicGroupFatalReason,
    ClassicGroupMachine, ClassicGroupPhase, ClassicGroupTransition, ClassicRejoinSchedule,
};

impl ClassicGroupMachine {
    pub(in crate::consumer::classic_group) fn rejoin_due(
        &mut self,
        schedule: ClassicRejoinSchedule,
        now: Moment,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        if self.phase != ClassicGroupPhase::WaitingToRejoin {
            return Err(ClassicGroupErrorKind::InvalidPhase);
        }
        if self.pending_rejoin != Some(schedule) {
            return Err(ClassicGroupErrorKind::RejoinMismatch);
        }
        if !schedule.due().is_elapsed_at(now) {
            return Err(ClassicGroupErrorKind::DeadlineNotElapsed);
        }
        if self.next_cycle.is_none() {
            return Ok(self.finish_fatal(ClassicGroupFatal::new(
                schedule.cycle(),
                schedule.assignment_generation(),
                ClassicGroupFatalReason::CycleExhausted,
            )));
        }
        let Some(deadline) =
            now.checked_deadline_after(self.rejoin_policy().attempt_timeout_ticks())
        else {
            return Ok(self.finish_fatal(ClassicGroupFatal::new(
                schedule.cycle(),
                schedule.assignment_generation(),
                ClassicGroupFatalReason::AttemptDeadlineOverflow,
            )));
        };
        self.start_cycle(now, deadline)
    }

    pub(super) fn wait_to_rejoin(&mut self, schedule: ClassicRejoinSchedule) {
        self.phase = ClassicGroupPhase::WaitingToRejoin;
        self.active_cycle = None;
        self.deadline = None;
        self.pending_rejoin = Some(schedule);
        self.clear_pending();
        self.heartbeat.disarm();
    }

    pub(super) fn finish_fatal(&mut self, fatal: ClassicGroupFatal) -> ClassicGroupTransition {
        self.retain_fatal(fatal);
        ClassicGroupTransition::one(ClassicGroupEffect::Fatal { fatal })
    }

    pub(super) fn retain_fatal(&mut self, fatal: ClassicGroupFatal) {
        self.phase = ClassicGroupPhase::Fatal;
        self.active_cycle = None;
        self.deadline = None;
        self.pending_rejoin = None;
        self.fatal = Some(fatal);
        self.clear_pending();
        self.heartbeat.disarm();
    }
}
