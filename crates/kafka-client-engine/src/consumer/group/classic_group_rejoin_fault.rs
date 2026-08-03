//! Linear retention of exact core effects after a due rejoin mutates policy state.

use kafka_client_core::{
    ClassicGroupEffect, ClassicGroupTiming, ClassicProtocol, Deadline, GroupId, MemberId,
    MembershipCycle,
};

use crate::clock::ClockError;

/// Exact scalar Join effect retained after the core has advanced.
#[must_use = "a core-emitted rejoin Join must be staged or frozen in its entry"]
#[expect(
    clippy::struct_field_names,
    reason = "unique authority-token prefixes prevent cross-owner field collisions"
)]
pub(super) struct PendingClassicRejoinJoin {
    pending_rejoin_group_id: GroupId,
    pending_rejoin_cycle: MembershipCycle,
    pending_rejoin_protocol: ClassicProtocol,
    pending_rejoin_member_id: Option<MemberId>,
    pending_rejoin_timing: ClassicGroupTiming,
    pending_rejoin_deadline: Deadline,
}

/// Concrete freeze point for any fallible work after `RejoinDue` mutates core.
#[must_use = "a post-core rejoin fault retains the exact emitted effect"]
#[expect(
    clippy::struct_field_names,
    reason = "unique authority-token prefixes prevent cross-owner field collisions"
)]
pub(super) struct ClassicRejoinPostCore {
    post_core_rejoin_join: Option<PendingClassicRejoinJoin>,
    post_core_rejoin_other: [Option<ClassicGroupEffect>; 2],
    post_core_rejoin_failure: ClassicRejoinPostCoreFailure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicRejoinPostCoreFailure {
    EffectShape,
    Identity,
    CycleSequence,
    MachineState,
    Clock(ClockError),
    ScheduleState,
    ExecutionOccupied,
}

impl PendingClassicRejoinJoin {
    pub(super) const fn new(
        group_id: GroupId,
        cycle: MembershipCycle,
        protocol: ClassicProtocol,
        member_id: Option<MemberId>,
        timing: ClassicGroupTiming,
        deadline: Deadline,
    ) -> Self {
        Self {
            pending_rejoin_group_id: group_id,
            pending_rejoin_cycle: cycle,
            pending_rejoin_protocol: protocol,
            pending_rejoin_member_id: member_id,
            pending_rejoin_timing: timing,
            pending_rejoin_deadline: deadline,
        }
    }

    pub(super) const fn group_id(&self) -> GroupId {
        self.pending_rejoin_group_id
    }

    pub(super) const fn cycle(&self) -> MembershipCycle {
        self.pending_rejoin_cycle
    }

    pub(super) const fn protocol(&self) -> ClassicProtocol {
        self.pending_rejoin_protocol
    }

    pub(super) const fn member_id(&self) -> Option<MemberId> {
        self.pending_rejoin_member_id
    }

    pub(super) const fn timing(&self) -> ClassicGroupTiming {
        self.pending_rejoin_timing
    }

    pub(super) const fn deadline(&self) -> Deadline {
        self.pending_rejoin_deadline
    }
}

impl ClassicRejoinPostCore {
    pub(super) const fn new(
        join: Option<PendingClassicRejoinJoin>,
        other: [Option<ClassicGroupEffect>; 2],
        failure: ClassicRejoinPostCoreFailure,
    ) -> Self {
        Self {
            post_core_rejoin_join: join,
            post_core_rejoin_other: other,
            post_core_rejoin_failure: failure,
        }
    }

    pub(super) fn retained_owner_count(&self) -> usize {
        let join = usize::from(self.post_core_rejoin_join.is_some());
        let first = usize::from(self.post_core_rejoin_other[0].is_some());
        let second = usize::from(self.post_core_rejoin_other[1].is_some());
        join.saturating_add(first).saturating_add(second).max(1)
    }

    #[cfg(test)]
    pub(super) const fn join(&self) -> Option<&PendingClassicRejoinJoin> {
        self.post_core_rejoin_join.as_ref()
    }

    #[cfg(test)]
    pub(super) const fn failure(&self) -> ClassicRejoinPostCoreFailure {
        self.post_core_rejoin_failure
    }

    #[cfg(test)]
    pub(super) const fn other(&self) -> &[Option<ClassicGroupEffect>; 2] {
        &self.post_core_rejoin_other
    }

    #[cfg(test)]
    pub(super) const fn join_for_test(
        group_id: GroupId,
        cycle: MembershipCycle,
        member_id: Option<MemberId>,
        timing: ClassicGroupTiming,
        deadline: Deadline,
        failure: ClassicRejoinPostCoreFailure,
    ) -> Self {
        Self::new(
            Some(PendingClassicRejoinJoin::new(
                group_id,
                cycle,
                ClassicProtocol::Range,
                member_id,
                timing,
                deadline,
            )),
            [None, None],
            failure,
        )
    }
}
