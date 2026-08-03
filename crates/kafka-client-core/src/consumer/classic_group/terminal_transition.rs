//! Terminal failure and close transitions for classic membership.

use crate::Moment;

use super::{
    ClassicGroupEffect, ClassicGroupErrorKind, ClassicGroupMachine, ClassicGroupPhase,
    ClassicGroupTransition, MembershipCycle,
    transition_support::{validate_inflight_cycle, validate_stage_cycle},
};

impl ClassicGroupMachine {
    pub(super) fn join_failed(
        &mut self,
        cycle: MembershipCycle,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        self.stage_failed(cycle, ClassicGroupPhase::Joining)
    }

    pub(super) fn partition_counts_failed(
        &mut self,
        cycle: MembershipCycle,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        self.stage_failed(cycle, ClassicGroupPhase::AwaitingPartitionCounts)
    }

    pub(super) fn sync_failed(
        &mut self,
        cycle: MembershipCycle,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        self.stage_failed(cycle, ClassicGroupPhase::Syncing)
    }

    pub(super) fn assignment_lost(
        &mut self,
        cycle: MembershipCycle,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        if self.live_cycle != Some(cycle) {
            return Err(ClassicGroupErrorKind::CycleMismatch);
        }
        if self.live_assignment.is_none() {
            return Err(ClassicGroupErrorKind::InvalidPhase);
        }
        self.revoke_stable_assignment()
    }

    pub(super) fn revoke_stable_assignment(
        &mut self,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        let revoke = self.take_stable_revoke()?;
        self.phase = ClassicGroupPhase::Lost;
        self.active_cycle = None;
        self.deadline = None;
        self.clear_pending();
        self.heartbeat.disarm();
        Ok(ClassicGroupTransition::one(revoke))
    }

    pub(super) fn take_stable_revoke(
        &mut self,
    ) -> Result<ClassicGroupEffect, ClassicGroupErrorKind> {
        let has_live = self.live_assignment.is_some();
        if has_live != self.live_generation.is_some() || has_live != self.live_cycle.is_some() {
            return Err(ClassicGroupErrorKind::InvariantViolation);
        }
        let assignment = self
            .live_assignment
            .take()
            .ok_or(ClassicGroupErrorKind::InvariantViolation)?;
        let classic_generation = self
            .live_generation
            .take()
            .ok_or(ClassicGroupErrorKind::InvariantViolation)?;
        self.live_cycle = None;
        Ok(ClassicGroupEffect::Revoke {
            assignment,
            classic_generation,
        })
    }

    pub(super) fn deadline_elapsed(
        &mut self,
        cycle: MembershipCycle,
        now: Moment,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        validate_inflight_cycle(self, cycle)?;
        let deadline = self
            .deadline
            .ok_or(ClassicGroupErrorKind::InvariantViolation)?;
        if !deadline.is_elapsed_at(now) {
            return Err(ClassicGroupErrorKind::DeadlineNotElapsed);
        }
        if self.live_assignment.is_some() {
            self.revoke_stable_assignment()
        } else {
            self.lose_cycle();
            Ok(ClassicGroupTransition::none())
        }
    }

    pub(super) fn close(&mut self) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        if self.phase == ClassicGroupPhase::Closed {
            return Err(ClassicGroupErrorKind::Closed);
        }
        let has_live = self.live_assignment.is_some();
        if has_live != self.live_generation.is_some() || has_live != self.live_cycle.is_some() {
            return Err(ClassicGroupErrorKind::InvariantViolation);
        }
        let revoke = match (self.live_assignment.take(), self.live_generation.take()) {
            (Some(assignment), Some(classic_generation)) => Some(ClassicGroupEffect::Revoke {
                assignment,
                classic_generation,
            }),
            (None, None) => None,
            _ => return Err(ClassicGroupErrorKind::InvariantViolation),
        };
        self.phase = ClassicGroupPhase::Closed;
        self.next_cycle = None;
        self.next_assignment_generation = None;
        self.live_cycle = None;
        self.active_cycle = None;
        self.deadline = None;
        self.pending_rejoin = None;
        self.clear_pending();
        self.heartbeat.disarm();
        Ok(revoke.map_or_else(ClassicGroupTransition::none, ClassicGroupTransition::one))
    }

    pub(super) fn lose_cycle(&mut self) {
        self.phase = ClassicGroupPhase::Lost;
        self.active_cycle = None;
        self.deadline = None;
        self.pending_rejoin = None;
        self.clear_pending();
        self.heartbeat.disarm();
    }

    fn stage_failed(
        &mut self,
        cycle: MembershipCycle,
        expected: ClassicGroupPhase,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        validate_stage_cycle(self, expected, cycle)?;
        if self.live_assignment.is_some() {
            self.revoke_stable_assignment()
        } else {
            self.lose_cycle();
            Ok(ClassicGroupTransition::none())
        }
    }
}
