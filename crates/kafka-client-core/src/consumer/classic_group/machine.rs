//! Unique owner of classic membership phase, fences, and live assignment.

use crate::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupId, LiveGroupAssignment,
    MemberId,
};

use super::{
    ClassicGeneration, ClassicGroupFatal, ClassicGroupPhase, ClassicGroupTiming,
    ClassicHeartbeatPolicy, ClassicJoinMembers, ClassicRejoinPolicy, ClassicRejoinSchedule,
    JoinedMemberSlot, MembershipCycle, heartbeat_state::ClassicHeartbeatState,
    reconciliation::PendingClassicReconciliation,
};

/// Deterministic owner for one group's classic Join and Sync lifecycle.
#[derive(Debug, Eq, PartialEq)]
pub struct ClassicGroupMachine {
    pub(super) group_id: GroupId,
    protocol: super::ClassicProtocol,
    timing: ClassicGroupTiming,
    rejoin_policy: ClassicRejoinPolicy,
    pub(super) phase: ClassicGroupPhase,
    pub(super) next_cycle: Option<MembershipCycle>,
    pub(super) active_cycle: Option<MembershipCycle>,
    pub(super) deadline: Option<Deadline>,
    pub(super) pending_member_id: Option<MemberId>,
    pub(super) pending_generation: Option<ClassicGeneration>,
    pub(super) pending_members: Option<ClassicJoinMembers>,
    pub(super) pending_local_slot: Option<JoinedMemberSlot>,
    pub(super) pending_expected_assignment: Option<Vec<GroupAssignmentPartition>>,
    pub(super) pending_heartbeat_liveness: Option<Deadline>,
    pub(super) next_assignment_generation: Option<AssignmentGeneration>,
    pub(super) live_cycle: Option<MembershipCycle>,
    pub(super) live_generation: Option<ClassicGeneration>,
    pub(super) live_assignment: Option<LiveGroupAssignment>,
    pub(super) pending_reconciliation: Option<PendingClassicReconciliation>,
    pub(super) pending_withheld_transfers: bool,
    pub(super) pending_rejoin: Option<ClassicRejoinSchedule>,
    pub(super) fatal: Option<ClassicGroupFatal>,
    pub(super) heartbeat: ClassicHeartbeatState,
}

impl ClassicGroupMachine {
    /// Creates one dormant owner without consulting time or emitting effects.
    pub const fn new(
        group_id: GroupId,
        timing: ClassicGroupTiming,
        heartbeat_policy: ClassicHeartbeatPolicy,
        rejoin_policy: ClassicRejoinPolicy,
    ) -> Self {
        Self::new_with_protocol(
            group_id,
            super::ClassicProtocol::Range,
            timing,
            heartbeat_policy,
            rejoin_policy,
        )
    }

    /// Creates one dormant owner with an immutable assignment protocol.
    pub const fn new_with_protocol(
        group_id: GroupId,
        protocol: super::ClassicProtocol,
        timing: ClassicGroupTiming,
        heartbeat_policy: ClassicHeartbeatPolicy,
        rejoin_policy: ClassicRejoinPolicy,
    ) -> Self {
        Self {
            group_id,
            protocol,
            timing,
            rejoin_policy,
            phase: ClassicGroupPhase::Dormant,
            next_cycle: Some(MembershipCycle::initial()),
            active_cycle: None,
            deadline: None,
            pending_member_id: None,
            pending_generation: None,
            pending_members: None,
            pending_local_slot: None,
            pending_expected_assignment: None,
            pending_heartbeat_liveness: None,
            next_assignment_generation: Some(AssignmentGeneration::initial()),
            live_cycle: None,
            live_generation: None,
            live_assignment: None,
            pending_reconciliation: None,
            pending_withheld_transfers: false,
            pending_rejoin: None,
            fatal: None,
            heartbeat: ClassicHeartbeatState::new(heartbeat_policy),
        }
    }

    /// Returns the stable engine-catalog group identity.
    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }

    /// Returns the immutable assignment protocol selected before admission.
    pub const fn protocol(&self) -> super::ClassicProtocol {
        self.protocol
    }

    /// Returns the immutable timeout policy emitted for every membership cycle.
    pub const fn timing(&self) -> ClassicGroupTiming {
        self.timing
    }

    /// Returns the immutable positive internal rejoin policy.
    pub const fn rejoin_policy(&self) -> ClassicRejoinPolicy {
        self.rejoin_policy
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

    /// Returns the membership cycle paired with the live assignment.
    pub const fn live_cycle(&self) -> Option<MembershipCycle> {
        self.live_cycle
    }

    /// Returns the exact Kafka generation paired with the live assignment.
    pub const fn live_generation(&self) -> Option<ClassicGeneration> {
        self.live_generation
    }

    /// Returns the exact pending rejoin schedule, if recovery is waiting.
    pub const fn pending_rejoin(&self) -> Option<ClassicRejoinSchedule> {
        self.pending_rejoin
    }

    /// Returns the retained terminal cause after membership becomes fatal.
    pub const fn fatal(&self) -> Option<ClassicGroupFatal> {
        self.fatal
    }
}
