//! Core-authorized classic-leader candidate, count, and Sync preparation.

use kafka_client_core::{
    ClassicGeneration, ClassicGroupEffect, ClassicGroupInput, ClassicGroupPhase, Moment,
};

use crate::{
    clock::OperationDeadline,
    protocol::consumer::{
        ClassicSyncMember, ClassicSyncRequestFailure, ClassicSyncTopic, classic_sync_group_request,
    },
};

use super::{
    classic_group_candidate::{ClassicGroupCycleCandidate, ClassicGroupCycleCandidateError},
    classic_group_owner::{ClassicGroupCandidateOwnershipError, ClassicGroupOwner},
    classic_group_partition_counts::PreparedClassicGroupPartitionCounts,
    classic_group_sync::{ClassicGroupSyncIdentity, PreparedClassicGroupSync},
    session_catalog::GroupSessionCatalog,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupLeaderJoinError {
    Candidate(ClassicGroupCandidateOwnershipError),
    CandidateFacts(ClassicGroupCycleCandidateError),
    MissingLocalSlot,
    Core(kafka_client_core::ClassicGroupErrorKind),
    UnexpectedCountEffect,
    CountStorage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupLeaderCountError {
    Fence,
    MissingTopic(kafka_client_core::TopicId),
    Allocation,
    Core(kafka_client_core::ClassicGroupErrorKind),
    UnexpectedSyncEffect,
    SyncRequest(ClassicSyncRequestFailure),
}

impl ClassicGroupOwner {
    pub(super) fn apply_leader_join(
        &mut self,
        candidate: ClassicGroupCycleCandidate,
        generation: ClassicGeneration,
        now: Moment,
        deadline: OperationDeadline,
    ) -> Result<PreparedClassicGroupPartitionCounts, ClassicGroupLeaderJoinError> {
        self.validate_candidate(&candidate)
            .map_err(ClassicGroupLeaderJoinError::Candidate)?;
        let cycle = candidate.cycle();
        let member_id = candidate.local_member_id();
        let local_slot = candidate
            .local_slot()
            .ok_or(ClassicGroupLeaderJoinError::MissingLocalSlot)?;
        let members = candidate
            .try_core_join_members()
            .map_err(ClassicGroupLeaderJoinError::CandidateFacts)?;
        self.pending = Some(candidate);
        let transition = self
            .apply(ClassicGroupInput::JoinLeader {
                cycle,
                now,
                member_id,
                local_slot,
                generation,
                members,
            })
            .map_err(|error| ClassicGroupLeaderJoinError::Core(error.kind()))?;
        let mut effects = transition.into_effects();
        match effects.next() {
            Some(ClassicGroupEffect::RequestPartitionCounts {
                cycle: effect_cycle,
                topics,
                deadline: effect_deadline,
            }) if effect_cycle == cycle
                && effect_deadline == deadline.core()
                && effects.next().is_none() =>
            {
                PreparedClassicGroupPartitionCounts::try_new(cycle, topics, deadline)
                    .map_err(|_error| ClassicGroupLeaderJoinError::CountStorage)
            }
            _ => Err(ClassicGroupLeaderJoinError::UnexpectedCountEffect),
        }
    }

    #[expect(
        clippy::maybe_infinite_iter,
        reason = "the flagged cycle calls return fixed scalar fences and perform no iteration"
    )]
    pub(super) fn apply_leader_partition_counts(
        &mut self,
        catalog: &GroupSessionCatalog,
        prepared: &PreparedClassicGroupPartitionCounts,
        now: Moment,
    ) -> Result<PreparedClassicGroupSync, ClassicGroupLeaderCountError> {
        let candidate = self
            .pending
            .as_ref()
            .filter(|candidate| {
                candidate.cycle() == prepared.cycle()
                    && self.machine().phase() == ClassicGroupPhase::AwaitingPartitionCounts
                    && self.machine().active_cycle() == Some(prepared.cycle())
                    && self.machine().deadline() == Some(prepared.deadline().core())
            })
            .ok_or(ClassicGroupLeaderCountError::Fence)?;
        let members = sync_members(candidate)?;
        let topics = sync_topics(candidate, catalog, prepared.topics())?;
        let local_member = candidate.local_member().clone();
        let cycle = prepared.cycle();
        let counts = prepared
            .try_clone_completed_counts()
            .map_err(|_error| ClassicGroupLeaderCountError::Allocation)?;
        let transition = self
            .apply(ClassicGroupInput::PartitionCounts { cycle, now, counts })
            .map_err(|error| ClassicGroupLeaderCountError::Core(error.kind()))?;
        let mut effects = transition.into_effects();
        let (group_id, member_id, generation, plan) = match effects.next() {
            Some(ClassicGroupEffect::Sync {
                group_id,
                cycle: effect_cycle,
                member_id,
                generation,
                plan,
                deadline,
            }) if effect_cycle == cycle
                && deadline == prepared.deadline().core()
                && effects.next().is_none() =>
            {
                (group_id, member_id, generation, plan)
            }
            _ => return Err(ClassicGroupLeaderCountError::UnexpectedSyncEffect),
        };
        let request = classic_sync_group_request(
            catalog.group(),
            &local_member,
            generation,
            plan,
            &members,
            &topics,
        )
        .map_err(ClassicGroupLeaderCountError::SyncRequest)?;
        Ok(PreparedClassicGroupSync::new(
            ClassicGroupSyncIdentity::new(
                group_id,
                cycle,
                member_id,
                generation,
                prepared.deadline(),
            ),
            request,
        ))
    }
}

fn sync_members(
    candidate: &ClassicGroupCycleCandidate,
) -> Result<Vec<ClassicSyncMember>, ClassicGroupLeaderCountError> {
    let mut members = Vec::new();
    members
        .try_reserve_exact(candidate.sync_members().count())
        .map_err(|_error| ClassicGroupLeaderCountError::Allocation)?;
    members.extend(
        candidate
            .sync_members()
            .map(|(slot, member)| ClassicSyncMember::new(slot, member.clone())),
    );
    Ok(members)
}

fn sync_topics(
    candidate: &ClassicGroupCycleCandidate,
    catalog: &GroupSessionCatalog,
    topic_ids: &[kafka_client_core::TopicId],
) -> Result<Vec<ClassicSyncTopic>, ClassicGroupLeaderCountError> {
    let mut topics = Vec::new();
    topics
        .try_reserve_exact(topic_ids.len())
        .map_err(|_error| ClassicGroupLeaderCountError::Allocation)?;
    for topic_id in topic_ids {
        let topic = candidate
            .topic_name(catalog, *topic_id)
            .ok_or(ClassicGroupLeaderCountError::MissingTopic(*topic_id))?;
        topics.push(ClassicSyncTopic::new(*topic_id, topic.clone()));
    }
    Ok(topics)
}
