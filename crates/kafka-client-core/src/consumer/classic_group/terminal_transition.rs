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
        validate_stage_cycle(self, ClassicGroupPhase::Stable, cycle)?;
        self.revoke_stable_assignment()
    }

    pub(super) fn revoke_stable_assignment(
        &mut self,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        if self.live_assignment.is_some() != self.live_generation.is_some() {
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
        self.phase = ClassicGroupPhase::Lost;
        self.active_cycle = None;
        self.deadline = None;
        self.clear_pending();
        self.heartbeat.disarm();
        Ok(ClassicGroupTransition::one(ClassicGroupEffect::Revoke {
            assignment,
            classic_generation,
        }))
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
        self.lose_cycle();
        Ok(ClassicGroupTransition::none())
    }

    pub(super) fn close(&mut self) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        if self.phase == ClassicGroupPhase::Closed {
            return Err(ClassicGroupErrorKind::Closed);
        }
        if self.live_assignment.is_some() != self.live_generation.is_some() {
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
        self.active_cycle = None;
        self.deadline = None;
        self.clear_pending();
        self.heartbeat.disarm();
        Ok(revoke.map_or_else(ClassicGroupTransition::none, ClassicGroupTransition::one))
    }

    pub(super) fn lose_cycle(&mut self) {
        self.phase = ClassicGroupPhase::Lost;
        self.active_cycle = None;
        self.deadline = None;
        self.clear_pending();
        self.heartbeat.disarm();
    }

    fn stage_failed(
        &mut self,
        cycle: MembershipCycle,
        expected: ClassicGroupPhase,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        validate_stage_cycle(self, expected, cycle)?;
        self.lose_cycle();
        Ok(ClassicGroupTransition::none())
    }
}
