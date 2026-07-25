//! Unique owner of classic membership phase, fences, and live assignment.

use crate::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupId, LiveGroupAssignment,
    MemberId,
};

use super::{
    ClassicGeneration, ClassicGroupPhase, ClassicGroupTiming, ClassicJoinMembers, JoinedMemberSlot,
    MembershipCycle,
};

/// Deterministic owner for one group's classic Join and Sync lifecycle.
#[derive(Debug, Eq, PartialEq)]
pub struct ClassicGroupMachine {
    pub(super) group_id: GroupId,
    timing: ClassicGroupTiming,
    pub(super) phase: ClassicGroupPhase,
    pub(super) next_cycle: Option<MembershipCycle>,
    pub(super) active_cycle: Option<MembershipCycle>,
    pub(super) deadline: Option<Deadline>,
    pub(super) pending_member_id: Option<MemberId>,
    pub(super) pending_generation: Option<ClassicGeneration>,
    pub(super) pending_members: Option<ClassicJoinMembers>,
    pub(super) pending_local_slot: Option<JoinedMemberSlot>,
    pub(super) pending_expected_assignment: Option<Vec<GroupAssignmentPartition>>,
    pub(super) next_assignment_generation: Option<AssignmentGeneration>,
    pub(super) live_generation: Option<ClassicGeneration>,
    pub(super) live_assignment: Option<LiveGroupAssignment>,
}

impl ClassicGroupMachine {
    /// Creates one dormant owner without consulting time or emitting effects.
    pub const fn new(group_id: GroupId, timing: ClassicGroupTiming) -> Self {
        Self {
            group_id,
            timing,
            phase: ClassicGroupPhase::Dormant,
            next_cycle: Some(MembershipCycle::initial()),
            active_cycle: None,
            deadline: None,
            pending_member_id: None,
            pending_generation: None,
            pending_members: None,
            pending_local_slot: None,
            pending_expected_assignment: None,
            next_assignment_generation: Some(AssignmentGeneration::initial()),
            live_generation: None,
            live_assignment: None,
        }
    }

    /// Returns the stable engine-catalog group identity.
    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }

    /// Returns the immutable timeout policy emitted for every membership cycle.
    pub const fn timing(&self) -> ClassicGroupTiming {
        self.timing
    }

    /// Returns the current lifecycle phase.
    pub const fn phase(&self) -> ClassicGroupPhase {
        self.phase
    }

    /// Returns the exact active membership cycle, if any.
    pub const fn active_cycle(&self) -> Option<MembershipCycle> {
        self.active_cycle
    }

    /// Returns the original active-cycle deadline, if any.
    pub const fn deadline(&self) -> Option<Deadline> {
        self.deadline
    }

    /// Borrows the assignment installed only by matching Sync success.
    pub const fn live_assignment(&self) -> Option<&LiveGroupAssignment> {
        self.live_assignment.as_ref()
    }

    /// Returns the exact Kafka generation paired with the live assignment.
    pub const fn live_generation(&self) -> Option<ClassicGeneration> {
        self.live_generation
    }
}
