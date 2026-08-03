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
        if self.pending_reconciliation.is_some() {
            return Err(ClassicGroupErrorKind::InvalidPhase);
        }
        if !schedule.due().is_elapsed_at(now) {
            return Err(ClassicGroupErrorKind::DeadlineNotElapsed);
        }
        if self.next_cycle.is_none() {
            return self.finish_fatal(ClassicGroupFatal::new(
                schedule.cycle(),
                schedule.assignment_generation(),
                ClassicGroupFatalReason::CycleExhausted,
            ));
        }
        let Some(deadline) =
            now.checked_deadline_after(self.rejoin_policy().attempt_timeout_ticks())
        else {
            return self.finish_fatal(ClassicGroupFatal::new(
                schedule.cycle(),
                schedule.assignment_generation(),
                ClassicGroupFatalReason::AttemptDeadlineOverflow,
            ));
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

    pub(super) fn wait_to_rejoin_after_reconciliation(&mut self, schedule: ClassicRejoinSchedule) {
        debug_assert_eq!(self.phase, ClassicGroupPhase::Reconciling);
        debug_assert!(self.pending_reconciliation.is_some());
        let pending_reconciliation = self.pending_reconciliation;
        self.wait_to_rejoin(schedule);
        self.pending_reconciliation = pending_reconciliation;
    }

    pub(in crate::consumer::classic_group) fn finish_fatal(
        &mut self,
        fatal: ClassicGroupFatal,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        let revoke = if self.live_assignment.is_some() {
            Some(self.take_stable_revoke()?)
        } else {
            None
        };
        self.retain_fatal(fatal);
        let effect = ClassicGroupEffect::Fatal { fatal };
        Ok(match revoke {
            Some(revoke) => ClassicGroupTransition::two(revoke, effect),
            None => ClassicGroupTransition::one(effect),
        })
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
