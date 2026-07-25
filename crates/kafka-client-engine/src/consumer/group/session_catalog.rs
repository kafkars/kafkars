//! Sole ownership of one classic-group spelling and committed assignment facts.

use std::{collections::BTreeMap, sync::Arc};

use kafka_client_core::{
    AssignmentGeneration, GroupId, LiveGroupAssignment, MemberId, PartitionIndex, TopicId,
};

use super::session_catalog_prepared::PreparedGroupSessionReplacement;

pub(super) const MAX_GROUP_SESSION_PARTITIONS: usize = 64;
pub(super) const MAX_GROUP_SESSION_TOPICS: usize = 64;
pub(super) const MAX_GROUP_SESSION_TOPIC_BYTES: usize = 249;
pub(super) const MAX_GROUP_SESSION_TOPIC_NAME_BYTES: usize = 16 * 1024;
pub(super) const MAX_KAFKA_GROUP_STRING_BYTES: usize = i16::MAX as usize;

/// Exact topic spelling and partition reported by group membership.
#[derive(Debug)]
pub(super) struct GroupSessionPartition {
    pub(super) topic: Arc<str>,
    pub(super) partition: PartitionIndex,
}

impl GroupSessionPartition {
    pub(super) const fn new(topic: Arc<str>, partition: PartitionIndex) -> Self {
        Self { topic, partition }
    }
}

pub(super) struct CurrentGroupSession {
    pub(super) member_id: MemberId,
    pub(super) member: Arc<str>,
    pub(super) classic_generation: i32,
    pub(super) assignment: LiveGroupAssignment,
}

/// Bounded staging or lookup failure that leaves the current session intact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupSessionCatalogError {
    EmptyGroup,
    GroupBytes { actual: usize, limit: usize },
    EmptyMember,
    MemberBytes { actual: usize, limit: usize },
    NegativeClassicGeneration { value: i32 },
    PartitionCapacity { actual: usize, limit: usize },
    PartitionOutOfRange { partition: PartitionIndex },
    EmptyTopic,
    TopicBytes { actual: usize, limit: usize },
    RetainedTopicCapacity { actual: usize, limit: usize },
    RetainedTopicBytes { actual: usize, limit: usize },
    RetainedTopicBytesOverflow,
    RetainedTopicCountOverflow,
    MemberIdentityExhausted,
    TopicIdentityExhausted,
    Assignment(kafka_client_core::LiveGroupAssignmentError),
    Allocation,
    UnknownTopic(TopicId),
}

/// One fixed group identity, persistent topic catalog, and current member.
pub(super) struct GroupSessionCatalog {
    group_id: GroupId,
    group: Arc<str>,
    pub(super) next_member_id: Option<MemberId>,
    pub(super) next_topic_id: Option<TopicId>,
    pub(super) retained_topic_name_bytes: usize,
    pub(super) topics_by_name: BTreeMap<Arc<str>, TopicId>,
    pub(super) topics_by_id: BTreeMap<TopicId, Arc<str>>,
    pub(super) current: Option<CurrentGroupSession>,
}

impl GroupSessionCatalog {
    pub(super) fn try_new(
        group_id: GroupId,
        group: Arc<str>,
    ) -> Result<Self, GroupSessionCatalogError> {
        validate_kafka_string(&group, GroupSessionCatalogError::EmptyGroup, |actual| {
            GroupSessionCatalogError::GroupBytes {
                actual,
                limit: MAX_KAFKA_GROUP_STRING_BYTES,
            }
        })?;
        Ok(Self {
            group_id,
            group,
            next_member_id: MemberId::try_from_raw(1),
            next_topic_id: Some(TopicId::from_raw(1)),
            retained_topic_name_bytes: 0,
            topics_by_name: BTreeMap::new(),
            topics_by_id: BTreeMap::new(),
            current: None,
        })
    }

    pub(super) fn prepare_replacement(
        &mut self,
        member: Arc<str>,
        classic_generation: i32,
        assignment_generation: AssignmentGeneration,
        partitions: Vec<GroupSessionPartition>,
    ) -> Result<PreparedGroupSessionReplacement<'_>, GroupSessionCatalogError> {
        PreparedGroupSessionReplacement::prepare(
            self,
            member,
            classic_generation,
            assignment_generation,
            partitions,
        )
    }

    pub(super) const fn group_id(&self) -> GroupId {
        self.group_id
    }

    pub(super) fn group(&self) -> &Arc<str> {
        &self.group
    }

    pub(super) fn current_member_id(&self) -> Option<MemberId> {
        self.current.as_ref().map(|current| current.member_id)
    }

    pub(super) fn current_member(&self) -> Option<&Arc<str>> {
        self.current.as_ref().map(|current| &current.member)
    }

    pub(super) fn classic_generation(&self) -> Option<i32> {
        self.current
            .as_ref()
            .map(|current| current.classic_generation)
    }

    pub(super) fn assignment_generation(&self) -> Option<AssignmentGeneration> {
        self.live_assignment()
            .map(LiveGroupAssignment::assignment_generation)
    }

    pub(super) fn live_assignment(&self) -> Option<&LiveGroupAssignment> {
        self.current.as_ref().map(|current| &current.assignment)
    }

    pub(super) fn topic_name(
        &self,
        topic_id: TopicId,
    ) -> Result<&Arc<str>, GroupSessionCatalogError> {
        self.topics_by_id
            .get(&topic_id)
            .ok_or(GroupSessionCatalogError::UnknownTopic(topic_id))
    }

    pub(super) fn retained_topic_count(&self) -> usize {
        self.topics_by_id.len()
    }

    pub(super) const fn retained_topic_name_bytes(&self) -> usize {
        self.retained_topic_name_bytes
    }

    pub(super) fn install_group_session_replacement(
        &mut self,
        staged_topics: BTreeMap<Arc<str>, TopicId>,
        next_member_id: Option<MemberId>,
        next_topic_id: Option<TopicId>,
        retained_topic_name_bytes: usize,
        current: CurrentGroupSession,
    ) {
        for (name, topic_id) in staged_topics {
            self.topics_by_name.insert(Arc::clone(&name), topic_id);
            self.topics_by_id.insert(topic_id, name);
        }
        self.next_member_id = next_member_id;
        self.next_topic_id = next_topic_id;
        self.retained_topic_name_bytes = retained_topic_name_bytes;
        self.current = Some(current);
    }

    #[cfg(test)]
    pub(super) fn set_identity_cursors_for_test(
        &mut self,
        member: Option<MemberId>,
        topic: Option<TopicId>,
    ) {
        self.next_member_id = member;
        self.next_topic_id = topic;
    }
}

pub(super) fn validate_kafka_string(
    value: &str,
    empty: GroupSessionCatalogError,
    too_long: impl FnOnce(usize) -> GroupSessionCatalogError,
) -> Result<(), GroupSessionCatalogError> {
    if value.is_empty() {
        return Err(empty);
    }
    if value.len() > MAX_KAFKA_GROUP_STRING_BYTES {
        return Err(too_long(value.len()));
    }
    Ok(())
}
