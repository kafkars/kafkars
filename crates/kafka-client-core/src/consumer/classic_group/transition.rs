//! Sole mutation owner for classic membership cycles and live assignment.

use crate::{Deadline, GroupAssignmentPartition, MemberId, Moment};

use super::{
    ClassicAssignmentPlan, ClassicGeneration, ClassicGroupEffect, ClassicGroupErrorKind,
    ClassicGroupMachine, ClassicGroupPhase, ClassicGroupTransition, ClassicJoinMembers,
    ClassicProtocol, JoinedMemberSlot, MembershipCycle, TopicPartitionCount,
    assignment::MAX_CLASSIC_MEMBER_PARTITIONS,
    transition_support::{
        collect_group_topics, copy_local_assignment, local_member_is_present, pair_error_kind,
        validate_active,
    },
};

impl ClassicGroupMachine {
    pub(super) fn begin(
        &mut self,
        now: Moment,
        deadline: Deadline,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        if self.phase == ClassicGroupPhase::Closed {
            return Err(ClassicGroupErrorKind::Closed);
        }
        if !matches!(
            self.phase,
            ClassicGroupPhase::Dormant | ClassicGroupPhase::Lost
        ) {
            return Err(ClassicGroupErrorKind::InvalidPhase);
        }
        self.start_cycle(now, deadline)
    }

    pub(super) fn start_cycle(
        &mut self,
        now: Moment,
        deadline: Deadline,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        if deadline.is_elapsed_at(now) {
            return Err(ClassicGroupErrorKind::DeadlineElapsed);
        }
        let cycle = self
            .next_cycle
            .ok_or(ClassicGroupErrorKind::CycleExhausted)?;
        let has_live = self.live_assignment.is_some();
        if has_live != self.live_generation.is_some() || has_live != self.live_cycle.is_some() {
            return Err(ClassicGroupErrorKind::InvariantViolation);
        }
        if has_live && self.protocol() != ClassicProtocol::CooperativeSticky {
            return Err(ClassicGroupErrorKind::InvariantViolation);
        }
        let retained_member_id = self
            .live_assignment()
            .map(crate::LiveGroupAssignment::member_id);
        let join = ClassicGroupEffect::Join {
            group_id: self.group_id,
            cycle,
            protocol: self.protocol(),
            member_id: retained_member_id,
            timing: self.timing(),
            deadline,
        };
        self.phase = ClassicGroupPhase::Joining;
        self.next_cycle = cycle.checked_next();
        self.active_cycle = Some(cycle);
        self.deadline = Some(deadline);
        self.pending_rejoin = None;
        self.clear_pending();
        self.pending_member_id = retained_member_id;
        self.heartbeat.disarm();
        Ok(ClassicGroupTransition::one(join))
    }

    pub(super) fn join_follower(
        &mut self,
        cycle: MembershipCycle,
        now: Moment,
        member_id: MemberId,
        generation: ClassicGeneration,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        let deadline = validate_active(self, ClassicGroupPhase::Joining, cycle, now)?;
        self.validate_join_member_id(member_id)?;
        let heartbeat_liveness = self.heartbeat_liveness_after(now)?;
        let effect = ClassicGroupEffect::Sync {
            group_id: self.group_id,
            cycle,
            member_id,
            generation,
            plan: ClassicAssignmentPlan::empty(),
            deadline,
        };
        self.phase = ClassicGroupPhase::Syncing;
        self.pending_member_id = Some(member_id);
        self.pending_generation = Some(generation);
        self.pending_local_slot = None;
        self.pending_expected_assignment = None;
        self.pending_heartbeat_liveness = Some(heartbeat_liveness);
        Ok(ClassicGroupTransition::one(effect))
    }

    pub(super) fn join_leader(
        &mut self,
        cycle: MembershipCycle,
        now: Moment,
        member_id: MemberId,
        local_slot: JoinedMemberSlot,
        generation: ClassicGeneration,
        members: ClassicJoinMembers,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        let deadline = validate_active(self, ClassicGroupPhase::Joining, cycle, now)?;
        self.validate_join_member_id(member_id)?;
        let heartbeat_liveness = self.heartbeat_liveness_after(now)?;
        if !local_member_is_present(&members, local_slot, member_id) {
            return Err(ClassicGroupErrorKind::LocalMemberMissing);
        }
        let topics = collect_group_topics(&members)?;
        let effect = ClassicGroupEffect::RequestPartitionCounts {
            cycle,
            topics,
            deadline,
        };
        self.phase = ClassicGroupPhase::AwaitingPartitionCounts;
        self.pending_member_id = Some(member_id);
        self.pending_generation = Some(generation);
        self.pending_members = Some(members);
        self.pending_local_slot = Some(local_slot);
        self.pending_expected_assignment = None;
        self.pending_heartbeat_liveness = Some(heartbeat_liveness);
        Ok(ClassicGroupTransition::one(effect))
    }

    pub(super) fn partition_counts(
        &mut self,
        cycle: MembershipCycle,
        now: Moment,
        counts: &[TopicPartitionCount],
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        let deadline =
            validate_active(self, ClassicGroupPhase::AwaitingPartitionCounts, cycle, now)?;
        let members = self
            .pending_members
            .as_ref()
            .ok_or(ClassicGroupErrorKind::InvariantViolation)?;
        let plan = match self.protocol() {
            ClassicProtocol::Range => ClassicAssignmentPlan::try_range(members, counts),
            ClassicProtocol::CooperativeSticky => {
                ClassicAssignmentPlan::try_cooperative_sticky(members, counts)
            }
        }
        .map_err(ClassicGroupErrorKind::Assignment)?;
        let local_slot = self
            .pending_local_slot
            .ok_or(ClassicGroupErrorKind::InvariantViolation)?;
        let expected_assignment = copy_local_assignment(&plan, local_slot)?;
        let withheld_transfers = plan.requires_followup();
        let member_id = self
            .pending_member_id
            .ok_or(ClassicGroupErrorKind::InvariantViolation)?;
        let generation = self
            .pending_generation
            .ok_or(ClassicGroupErrorKind::InvariantViolation)?;
        let effect = ClassicGroupEffect::Sync {
            group_id: self.group_id,
            cycle,
            member_id,
            generation,
            plan,
            deadline,
        };
        self.phase = ClassicGroupPhase::Syncing;
        self.pending_members = None;
        self.pending_local_slot = None;
        self.pending_expected_assignment = Some(expected_assignment);
        self.pending_withheld_transfers = withheld_transfers;
        Ok(ClassicGroupTransition::one(effect))
    }

    pub(super) fn sync_succeeded(
        &mut self,
        cycle: MembershipCycle,
        now: Moment,
        partitions: Vec<GroupAssignmentPartition>,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        validate_active(self, ClassicGroupPhase::Syncing, cycle, now)?;
        if partitions.len() > MAX_CLASSIC_MEMBER_PARTITIONS {
            return Err(ClassicGroupErrorKind::LocalAssignmentTooLarge);
        }
        if self
            .pending_expected_assignment
            .as_ref()
            .is_some_and(|expected| expected.as_slice() != partitions.as_slice())
        {
            return Err(ClassicGroupErrorKind::LeaderAssignmentMismatch);
        }
        let member_id = self
            .pending_member_id
            .ok_or(ClassicGroupErrorKind::InvariantViolation)?;
        let classic_generation = self
            .pending_generation
            .ok_or(ClassicGroupErrorKind::InvariantViolation)?;
        let assignment_generation = self
            .next_assignment_generation
            .ok_or(ClassicGroupErrorKind::AssignmentGenerationExhausted)?;
        let heartbeat_liveness = self
            .pending_heartbeat_liveness
            .ok_or(ClassicGroupErrorKind::InvariantViolation)?;
        let Some(heartbeat) = self.heartbeat.prepare_activation(
            cycle,
            assignment_generation,
            now,
            heartbeat_liveness,
        )?
        else {
            return if self.live_assignment.is_some() {
                self.revoke_stable_assignment()
            } else {
                self.lose_cycle();
                Ok(ClassicGroupTransition::none())
            };
        };
        if self.live_assignment.is_some() {
            return self.sync_cooperative_replacement(
                cycle,
                member_id,
                classic_generation,
                assignment_generation,
                partitions,
                heartbeat,
            );
        }
        let (retained, effect_assignment) = crate::LiveGroupAssignment::try_new_pair(
            self.group_id,
            member_id,
            assignment_generation,
            partitions,
        )
        .map_err(|(error, _)| pair_error_kind(error))?;
        self.phase = ClassicGroupPhase::Stable;
        self.deadline = None;
        self.clear_pending();
        self.next_assignment_generation = assignment_generation.checked_next();
        self.live_cycle = Some(cycle);
        self.live_generation = Some(classic_generation);
        self.live_assignment = Some(retained);
        self.heartbeat.activate(heartbeat);
        Ok(ClassicGroupTransition::one(ClassicGroupEffect::Install {
            assignment: effect_assignment,
            classic_generation,
            heartbeat,
        }))
    }

    pub(super) fn clear_pending(&mut self) {
        self.pending_member_id = None;
        self.pending_generation = None;
        self.pending_members = None;
        self.pending_local_slot = None;
        self.pending_expected_assignment = None;
        self.pending_heartbeat_liveness = None;
        self.pending_reconciliation = None;
        self.pending_withheld_transfers = false;
    }

    fn heartbeat_liveness_after(&self, now: Moment) -> Result<Deadline, ClassicGroupErrorKind> {
        let ticks = self.timing().session_timeout_ticks();
        now.checked_deadline_after(ticks)
            .ok_or(ClassicGroupErrorKind::DeadlineOverflow)
    }
}
