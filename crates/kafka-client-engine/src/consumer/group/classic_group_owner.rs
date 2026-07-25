//! Unique engine ownership of one deterministic classic-group machine.

use kafka_client_core::{
    ClassicGroupApplyError, ClassicGroupInput, ClassicGroupMachine, ClassicGroupPhase,
    ClassicGroupTiming, ClassicGroupTransition, ClassicHeartbeatPolicy, ClassicRejoinPolicy,
    GroupId,
};

use super::classic_group_candidate::ClassicGroupCycleCandidate;

/// One per-entry deterministic membership owner.
pub(super) struct ClassicGroupOwner {
    machine: ClassicGroupMachine,
    pub(super) pending: Option<ClassicGroupCycleCandidate>,
}

impl ClassicGroupOwner {
    pub(super) const fn new(
        group_id: GroupId,
        timing: ClassicGroupTiming,
        heartbeat_policy: ClassicHeartbeatPolicy,
        rejoin_policy: ClassicRejoinPolicy,
    ) -> Self {
        Self {
            machine: ClassicGroupMachine::new(group_id, timing, heartbeat_policy, rejoin_policy),
            pending: None,
        }
    }

    pub(super) const fn machine(&self) -> &ClassicGroupMachine {
        &self.machine
    }

    pub(super) fn apply(
        &mut self,
        input: ClassicGroupInput,
    ) -> Result<ClassicGroupTransition, ClassicGroupApplyError> {
        let transition = self.machine.apply(input)?;
        if matches!(
            self.machine.phase(),
            ClassicGroupPhase::Lost | ClassicGroupPhase::Closed
        ) {
            self.pending = None;
        }
        Ok(transition)
    }

    pub(super) const fn is_dormant(&self) -> bool {
        matches!(self.machine.phase(), ClassicGroupPhase::Dormant)
    }

    pub(super) fn stage_candidate(
        &mut self,
        candidate: ClassicGroupCycleCandidate,
    ) -> Result<(), ClassicGroupCandidateOwnershipError> {
        self.validate_candidate(&candidate)?;
        self.pending = Some(candidate);
        Ok(())
    }

    pub(super) fn pending(&self) -> Option<&ClassicGroupCycleCandidate> {
        self.pending.as_ref()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupCandidateOwnershipError {
    Phase,
    Cycle,
    Occupied,
}
