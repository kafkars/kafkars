//! Registry-selected atomic staging of owned Join and Sync cycle candidates.

use std::sync::Arc;

use kafka_client_core::{ClassicJoinMembers, GroupId, JoinedMemberSlot, MemberId, MembershipCycle};

use super::{
    classic_group_candidate::{ClassicGroupCycleCandidateError, JoinedGroupMember},
    classic_group_owner::ClassicGroupCandidateOwnershipError,
    registry::GroupConsumerRegistry,
};

pub(super) struct StagedLeaderCycle {
    pub(super) member_id: MemberId,
    pub(super) local_slot: JoinedMemberSlot,
    pub(super) members: ClassicJoinMembers,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerSessionFailure {
    UnknownGroup,
    Closing,
    Candidate(ClassicGroupCycleCandidateError),
    Ownership(ClassicGroupCandidateOwnershipError),
}

impl GroupConsumerRegistry {
    pub(super) fn stage_follower_cycle(
        &mut self,
        group_id: GroupId,
        cycle: MembershipCycle,
        local_member: Arc<str>,
    ) -> Result<MemberId, GroupConsumerSessionFailure> {
        let entry = self.entry_mut(group_id)?;
        if !entry.is_active() {
            return Err(GroupConsumerSessionFailure::Closing);
        }
        let candidate = entry
            .catalog
            .prepare_follower_cycle(cycle, local_member)
            .map_err(GroupConsumerSessionFailure::Candidate)?;
        let member_id = candidate.local_member_id();
        entry
            .classic
            .stage_candidate(candidate)
            .map_err(GroupConsumerSessionFailure::Ownership)?;
        Ok(member_id)
    }

    pub(super) fn stage_leader_cycle(
        &mut self,
        group_id: GroupId,
        cycle: MembershipCycle,
        local_member: Arc<str>,
        joined: Vec<JoinedGroupMember>,
    ) -> Result<StagedLeaderCycle, GroupConsumerSessionFailure> {
        let entry = self.entry_mut(group_id)?;
        if !entry.is_active() {
            return Err(GroupConsumerSessionFailure::Closing);
        }
        let candidate = entry
            .catalog
            .prepare_leader_cycle(cycle, local_member, joined)
            .map_err(GroupConsumerSessionFailure::Candidate)?;
        let members = candidate
            .try_core_join_members()
            .map_err(GroupConsumerSessionFailure::Candidate)?;
        let member_id = candidate.local_member_id();
        let local_slot = candidate
            .local_slot()
            .ok_or(GroupConsumerSessionFailure::Candidate(
                ClassicGroupCycleCandidateError::LocalMemberMissing,
            ))?;
        entry
            .classic
            .stage_candidate(candidate)
            .map_err(GroupConsumerSessionFailure::Ownership)?;
        Ok(StagedLeaderCycle {
            member_id,
            local_slot,
            members,
        })
    }

    fn entry_mut(
        &mut self,
        group_id: GroupId,
    ) -> Result<&mut super::registry_entry::GroupConsumerEntry, GroupConsumerSessionFailure> {
        self.entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .ok_or(GroupConsumerSessionFailure::UnknownGroup)
    }
}
