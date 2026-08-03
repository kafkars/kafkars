//! Owned staged identity and spelling facts for one Join and Sync cycle.

use std::{collections::BTreeMap, sync::Arc};

use kafka_client_core::{
    ClassicGeneration, ClassicJoinMember, ClassicJoinMembers, ClassicJoinMembersError,
    ClassicSubscription, ClassicSubscriptionError, GroupAssignmentPartition, JoinedMemberSlot,
    MemberId, MemberRank, MembershipCycle, TopicId,
};

use super::session_catalog::GroupSessionCatalog;

/// Join-boundary spelling retained without admitting protocol ownership into candidate policy.
pub(super) struct JoinedOwnedPartition {
    pub(super) topic: Arc<str>,
    pub(super) partition: i32,
}

impl JoinedOwnedPartition {
    pub(super) const fn new(topic: Arc<str>, partition: i32) -> Self {
        Self { topic, partition }
    }

    pub(super) fn into_parts(self) -> (Arc<str>, i32) {
        (self.topic, self.partition)
    }
}

/// One decoded member spelling and subscription before core normalization.
pub(super) struct JoinedGroupMember {
    pub(super) slot: JoinedMemberSlot,
    pub(super) member: Arc<str>,
    pub(super) topics: Vec<Arc<str>>,
    pub(super) owned_partitions: Vec<JoinedOwnedPartition>,
    pub(super) generation: Option<ClassicGeneration>,
}

impl JoinedGroupMember {
    pub(super) const fn new(
        slot: JoinedMemberSlot,
        member: Arc<str>,
        topics: Vec<Arc<str>>,
    ) -> Self {
        Self {
            slot,
            member,
            topics,
            owned_partitions: Vec::new(),
            generation: None,
        }
    }

    pub(super) const fn new_with_owned(
        slot: JoinedMemberSlot,
        member: Arc<str>,
        topics: Vec<Arc<str>>,
        owned_partitions: Vec<JoinedOwnedPartition>,
        generation: Option<ClassicGeneration>,
    ) -> Self {
        Self {
            slot,
            member,
            topics,
            owned_partitions,
            generation,
        }
    }
}

pub(super) struct CandidateMember {
    joined_slot: JoinedMemberSlot,
    normalized_member_id: MemberId,
    ordering_rank: MemberRank,
    kafka_member_spelling: Arc<str>,
    subscribed_topic_ids: Vec<TopicId>,
    candidate_owned_partitions: Vec<GroupAssignmentPartition>,
    candidate_generation: Option<ClassicGeneration>,
}

impl CandidateMember {
    pub(super) const fn from_prepared_member_with_owned(
        slot: JoinedMemberSlot,
        member_id: MemberId,
        rank: MemberRank,
        member: Arc<str>,
        topics: Vec<TopicId>,
        owned_partitions: Vec<GroupAssignmentPartition>,
        generation: Option<ClassicGeneration>,
    ) -> Self {
        Self {
            joined_slot: slot,
            normalized_member_id: member_id,
            ordering_rank: rank,
            kafka_member_spelling: member,
            subscribed_topic_ids: topics,
            candidate_owned_partitions: owned_partitions,
            candidate_generation: generation,
        }
    }
}

pub(super) struct PreparedClassicGroupCycle {
    pub(super) local_member_id: MemberId,
    pub(super) local_member: Arc<str>,
    pub(super) local_slot: Option<JoinedMemberSlot>,
    pub(super) members: Vec<CandidateMember>,
    pub(super) staged_topics: BTreeMap<Arc<str>, TopicId>,
    pub(super) next_member_id: Option<MemberId>,
    pub(super) next_topic_id: Option<TopicId>,
    pub(super) retained_topic_name_bytes: usize,
}

/// Linear candidate retained across Join, partition-count, and Sync turns.
#[must_use = "a classic-group cycle candidate must be installed or dropped"]
pub(super) struct ClassicGroupCycleCandidate {
    membership_cycle: MembershipCycle,
    local_catalog_member_id: MemberId,
    local_kafka_member: Arc<str>,
    local_joined_slot: Option<JoinedMemberSlot>,
    ranked_members: Vec<CandidateMember>,
    foreign_topic_bindings: BTreeMap<Arc<str>, TopicId>,
    member_cursor_after_install: Option<MemberId>,
    topic_cursor_after_install: Option<TopicId>,
    retained_topic_bytes_after_install: usize,
    base_member_cursor: Option<MemberId>,
    base_topic_cursor: Option<TopicId>,
    base_topic_count: usize,
    base_topic_name_bytes: usize,
    local_topic_ids: Vec<TopicId>,
}

pub(super) type ClassicGroupCatalogInstall = (
    BTreeMap<Arc<str>, TopicId>,
    Option<MemberId>,
    Option<TopicId>,
    usize,
    Arc<str>,
);

impl ClassicGroupCycleCandidate {
    pub(super) fn try_from_prepared_cycle(
        catalog: &GroupSessionCatalog,
        cycle: MembershipCycle,
        prepared: PreparedClassicGroupCycle,
    ) -> Result<Self, ClassicGroupCycleCandidateError> {
        let mut local_subscription = Vec::new();
        local_subscription
            .try_reserve_exact(catalog.local_subscription().len())
            .map_err(|_error| ClassicGroupCycleCandidateError::Allocation)?;
        local_subscription.extend_from_slice(catalog.local_subscription());
        Ok(Self {
            membership_cycle: cycle,
            local_catalog_member_id: prepared.local_member_id,
            local_kafka_member: prepared.local_member,
            local_joined_slot: prepared.local_slot,
            ranked_members: prepared.members,
            foreign_topic_bindings: prepared.staged_topics,
            member_cursor_after_install: prepared.next_member_id,
            topic_cursor_after_install: prepared.next_topic_id,
            retained_topic_bytes_after_install: prepared.retained_topic_name_bytes,
            base_member_cursor: catalog.next_member_id,
            base_topic_cursor: catalog.next_topic_id,
            base_topic_count: catalog.retained_topic_count(),
            base_topic_name_bytes: catalog.retained_topic_name_bytes(),
            local_topic_ids: local_subscription,
        })
    }

    pub(super) const fn cycle(&self) -> MembershipCycle {
        self.membership_cycle
    }

    pub(super) const fn local_member_id(&self) -> MemberId {
        self.local_catalog_member_id
    }

    pub(super) fn local_member(&self) -> &Arc<str> {
        &self.local_kafka_member
    }

    pub(super) const fn local_slot(&self) -> Option<JoinedMemberSlot> {
        self.local_joined_slot
    }

    pub(super) fn member_spelling(&self, slot: JoinedMemberSlot) -> Option<&Arc<str>> {
        self.ranked_members
            .iter()
            .find(|member| member.joined_slot == slot)
            .map(|member| &member.kafka_member_spelling)
    }

    pub(super) fn sync_members(&self) -> impl Iterator<Item = (JoinedMemberSlot, &Arc<str>)> {
        self.ranked_members
            .iter()
            .map(|member| (member.joined_slot, &member.kafka_member_spelling))
    }

    pub(super) fn topic_name<'a>(
        &'a self,
        catalog: &'a GroupSessionCatalog,
        topic_id: TopicId,
    ) -> Option<&'a Arc<str>> {
        self.foreign_topic_bindings
            .iter()
            .find_map(|(name, staged_id)| (*staged_id == topic_id).then_some(name))
            .or_else(|| catalog.topic_name(topic_id).ok())
    }

    pub(super) fn try_core_join_members(
        &self,
    ) -> Result<ClassicJoinMembers, ClassicGroupCycleCandidateError> {
        let mut joined = Vec::new();
        joined
            .try_reserve_exact(self.ranked_members.len())
            .map_err(|_error| ClassicGroupCycleCandidateError::Allocation)?;
        for member in &self.ranked_members {
            let mut topics = Vec::new();
            topics
                .try_reserve_exact(member.subscribed_topic_ids.len())
                .map_err(|_error| ClassicGroupCycleCandidateError::Allocation)?;
            topics.extend_from_slice(&member.subscribed_topic_ids);
            let mut owned_partitions = Vec::new();
            owned_partitions
                .try_reserve_exact(member.candidate_owned_partitions.len())
                .map_err(|_error| ClassicGroupCycleCandidateError::Allocation)?;
            owned_partitions.extend_from_slice(&member.candidate_owned_partitions);
            let subscription = ClassicSubscription::try_new_with_owned(
                topics,
                owned_partitions,
                member.candidate_generation,
            )
            .map_err(ClassicGroupCycleCandidateError::Subscription)?;
            joined.push(ClassicJoinMember::new(
                member.joined_slot,
                member.normalized_member_id,
                member.ordering_rank,
                subscription,
            ));
        }
        ClassicJoinMembers::try_new(joined).map_err(ClassicGroupCycleCandidateError::Members)
    }

    pub(super) fn matches_catalog_base(&self, catalog: &GroupSessionCatalog) -> bool {
        self.base_member_cursor == catalog.next_member_id
            && self.base_topic_cursor == catalog.next_topic_id
            && self.base_topic_count == catalog.retained_topic_count()
            && self.base_topic_name_bytes == catalog.retained_topic_name_bytes()
    }

    pub(super) fn local_owns_topic(&self, topic_id: TopicId) -> bool {
        self.local_topic_ids.binary_search(&topic_id).is_ok()
    }

    pub(super) fn into_catalog_install(self) -> ClassicGroupCatalogInstall {
        (
            self.foreign_topic_bindings,
            self.member_cursor_after_install,
            self.topic_cursor_after_install,
            self.retained_topic_bytes_after_install,
            self.local_kafka_member,
        )
    }

    #[cfg(test)]
    pub(super) const fn next_member_id_after_install(&self) -> Option<MemberId> {
        self.member_cursor_after_install
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupCycleCandidateError {
    Allocation,
    InvalidOwnedPartition,
    DuplicateOwnedPartition,
    EmptyMember,
    MemberBytes { actual: usize, limit: usize },
    MemberCapacity { actual: usize, limit: usize },
    DuplicateMember,
    DuplicateSlot(JoinedMemberSlot),
    LocalMemberMissing,
    LocalSubscriptionMismatch,
    LocalOwnershipMismatch,
    RequiredMemberMismatch,
    TopicCapacity { actual: usize, limit: usize },
    TopicsPerMember { actual: usize, limit: usize },
    DuplicateTopic,
    Catalog(super::session_catalog::GroupSessionCatalogError),
    MemberIdentityExhausted,
    TopicIdentityExhausted,
    RankExhausted,
    Subscription(ClassicSubscriptionError),
    Members(ClassicJoinMembersError),
}
