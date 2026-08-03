//! Cooperative assignment application fencing and follow-up cycle transitions.

use crate::{AssignmentGeneration, GroupAssignmentPartition, MemberId, Moment};

use super::{
    ClassicGeneration, ClassicGroupEffect, ClassicGroupErrorKind, ClassicGroupFatal,
    ClassicGroupFatalReason, ClassicGroupMachine, ClassicGroupPhase, ClassicGroupTransition,
    ClassicHeartbeatSchedule, ClassicProtocol, MembershipCycle,
    reconciliation::{PendingClassicReconciliation, try_prepare_reconciliation},
};

impl ClassicGroupMachine {
    pub(super) fn reconciliation_applied(
        &mut self,
        cycle: MembershipCycle,
        assignment_generation: AssignmentGeneration,
        now: Moment,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        let rejoin_is_already_scheduled = match self.phase {
            ClassicGroupPhase::Reconciling => false,
            ClassicGroupPhase::WaitingToRejoin if self.pending_rejoin.is_some() => true,
            _ => return Err(ClassicGroupErrorKind::InvalidPhase),
        };
        let pending = self
            .pending_reconciliation
            .ok_or(ClassicGroupErrorKind::InvariantViolation)?;
        if pending.cycle() != cycle || self.live_cycle != Some(cycle) {
            return Err(ClassicGroupErrorKind::CycleMismatch);
        }
        if pending.assignment_generation() != assignment_generation
            || self.live_assignment().is_none_or(|assignment| {
                assignment.assignment_generation() != assignment_generation
            })
        {
            return Err(ClassicGroupErrorKind::HeartbeatMismatch);
        }
        if rejoin_is_already_scheduled {
            self.pending_reconciliation = None;
            return Ok(ClassicGroupTransition::none());
        }
        if !pending.requires_followup() {
            self.phase = ClassicGroupPhase::Stable;
            self.pending_reconciliation = None;
            return Ok(ClassicGroupTransition::none());
        }
        if self.next_cycle.is_none() {
            return self.finish_fatal(ClassicGroupFatal::new(
                cycle,
                Some(assignment_generation),
                ClassicGroupFatalReason::CycleExhausted,
            ));
        }
        let Some(deadline) =
            now.checked_deadline_after(self.rejoin_policy().attempt_timeout_ticks())
        else {
            return self.finish_fatal(ClassicGroupFatal::new(
                cycle,
                Some(assignment_generation),
                ClassicGroupFatalReason::AttemptDeadlineOverflow,
            ));
        };
        self.start_cycle(now, deadline)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn sync_cooperative_replacement(
        &mut self,
        cycle: MembershipCycle,
        member_id: MemberId,
        classic_generation: ClassicGeneration,
        assignment_generation: AssignmentGeneration,
        partitions: Vec<GroupAssignmentPartition>,
        heartbeat: ClassicHeartbeatSchedule,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        if self.protocol() != ClassicProtocol::CooperativeSticky {
            return Err(ClassicGroupErrorKind::InvariantViolation);
        }
        let previous_cycle = self
            .live_cycle
            .ok_or(ClassicGroupErrorKind::InvariantViolation)?;
        let previous_classic_generation = self
            .live_generation
            .ok_or(ClassicGroupErrorKind::InvariantViolation)?;
        let previous_assignment = self
            .live_assignment
            .as_ref()
            .ok_or(ClassicGroupErrorKind::InvariantViolation)?;
        let prepared = try_prepare_reconciliation(
            previous_cycle,
            previous_classic_generation,
            previous_assignment,
            cycle,
            classic_generation,
            member_id,
            assignment_generation,
            partitions,
            heartbeat,
            self.pending_withheld_transfers,
        )?;
        let requires_followup = prepared.effect.requires_followup();
        self.phase = ClassicGroupPhase::Reconciling;
        self.deadline = None;
        self.clear_pending();
        self.pending_reconciliation = Some(PendingClassicReconciliation::new(
            cycle,
            assignment_generation,
            requires_followup,
        ));
        self.next_assignment_generation = assignment_generation.checked_next();
        self.live_cycle = Some(cycle);
        self.live_generation = Some(classic_generation);
        self.live_assignment = Some(prepared.retained_assignment);
        self.heartbeat.activate(heartbeat);
        Ok(ClassicGroupTransition::one(ClassicGroupEffect::Reconcile {
            reconciliation: prepared.effect,
        }))
    }
}
