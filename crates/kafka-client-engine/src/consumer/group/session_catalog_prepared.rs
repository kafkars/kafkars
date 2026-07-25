//! Linear two-phase replacement of one bounded classic-group assignment.

use std::{collections::BTreeMap, sync::Arc};

use kafka_client_core::{
    AssignmentGeneration, GroupAssignmentPartition, LiveGroupAssignment, MemberId, TopicId,
};

use super::session_catalog::{
    CurrentGroupSession, GroupSessionCatalog, GroupSessionCatalogError, GroupSessionPartition,
    MAX_GROUP_SESSION_PARTITIONS, MAX_GROUP_SESSION_TOPIC_BYTES,
    MAX_GROUP_SESSION_TOPIC_NAME_BYTES, MAX_GROUP_SESSION_TOPICS, validate_kafka_string,
};

/// Candidate session facts that do not mutate the catalog until committed.
#[must_use = "a prepared group session replacement must be committed or dropped"]
pub(super) struct PreparedGroupSessionReplacement<'a> {
    owner: &'a mut GroupSessionCatalog,
    staged_topics: BTreeMap<Arc<str>, TopicId>,
    next_member_id: Option<MemberId>,
    next_topic_id: Option<TopicId>,
    retained_topic_name_bytes: usize,
    current: CurrentGroupSession,
}

impl<'a> PreparedGroupSessionReplacement<'a> {
    pub(super) fn prepare(
        owner: &'a mut GroupSessionCatalog,
        member: Arc<str>,
        classic_generation: i32,
        assignment_generation: AssignmentGeneration,
        partitions: Vec<GroupSessionPartition>,
    ) -> Result<Self, GroupSessionCatalogError> {
        validate_kafka_string(&member, GroupSessionCatalogError::EmptyMember, |actual| {
            GroupSessionCatalogError::MemberBytes {
                actual,
                limit: super::session_catalog::MAX_KAFKA_GROUP_STRING_BYTES,
            }
        })?;
        if classic_generation < 0 {
            return Err(GroupSessionCatalogError::NegativeClassicGeneration {
                value: classic_generation,
            });
        }
        if partitions.len() > MAX_GROUP_SESSION_PARTITIONS {
            return Err(GroupSessionCatalogError::PartitionCapacity {
                actual: partitions.len(),
                limit: MAX_GROUP_SESSION_PARTITIONS,
            });
        }
        let Some(member_id) = owner.next_member_id else {
            return Err(GroupSessionCatalogError::MemberIdentityExhausted);
        };
        let next_member_id = member_id
            .get()
            .checked_add(1)
            .and_then(MemberId::try_from_raw);
        let mut assignment_partitions = Vec::new();
        assignment_partitions
            .try_reserve_exact(partitions.len())
            .map_err(|_error| GroupSessionCatalogError::Allocation)?;
        let mut prepared = PreparedTopics::new(owner);
        for partition in partitions {
            if i32::try_from(partition.partition.get()).is_err() {
                return Err(GroupSessionCatalogError::PartitionOutOfRange {
                    partition: partition.partition,
                });
            }
            let topic_id = prepared.stage_topic(partition.topic)?;
            assignment_partitions
                .push(GroupAssignmentPartition::new(topic_id, partition.partition));
        }
        assignment_partitions.sort_unstable();
        let assignment = LiveGroupAssignment::try_new(
            prepared.owner.group_id(),
            member_id,
            assignment_generation,
            assignment_partitions,
        )
        .map_err(GroupSessionCatalogError::Assignment)?;
        Ok(Self {
            owner: prepared.owner,
            staged_topics: prepared.staged_topics,
            next_member_id,
            next_topic_id: prepared.next_topic_id,
            retained_topic_name_bytes: prepared.retained_topic_name_bytes,
            current: CurrentGroupSession {
                member_id,
                member,
                classic_generation,
                assignment,
            },
        })
    }

    pub(super) const fn live_assignment(&self) -> &LiveGroupAssignment {
        &self.current.assignment
    }

    pub(super) const fn member_id(&self) -> MemberId {
        self.current.member_id
    }

    pub(super) fn member(&self) -> &Arc<str> {
        &self.current.member
    }

    pub(super) const fn classic_generation(&self) -> i32 {
        self.current.classic_generation
    }

    pub(super) fn topic_name(&self, topic_id: TopicId) -> Option<&Arc<str>> {
        self.staged_topics
            .iter()
            .find_map(|(name, staged_id)| (*staged_id == topic_id).then_some(name))
            .or_else(|| self.owner.topics_by_id.get(&topic_id))
    }

    pub(super) fn commit(self) {
        self.owner.install_group_session_replacement(
            self.staged_topics,
            self.next_member_id,
            self.next_topic_id,
            self.retained_topic_name_bytes,
            self.current,
        );
    }
}

struct PreparedTopics<'a> {
    owner: &'a mut GroupSessionCatalog,
    staged_topics: BTreeMap<Arc<str>, TopicId>,
    next_topic_id: Option<TopicId>,
    retained_topic_name_bytes: usize,
}

impl<'a> PreparedTopics<'a> {
    fn new(owner: &'a mut GroupSessionCatalog) -> Self {
        Self {
            next_topic_id: owner.next_topic_id,
            retained_topic_name_bytes: owner.retained_topic_name_bytes,
            staged_topics: BTreeMap::new(),
            owner,
        }
    }

    fn stage_topic(&mut self, topic: Arc<str>) -> Result<TopicId, GroupSessionCatalogError> {
        if topic.is_empty() {
            return Err(GroupSessionCatalogError::EmptyTopic);
        }
        if topic.len() > MAX_GROUP_SESSION_TOPIC_BYTES {
            return Err(GroupSessionCatalogError::TopicBytes {
                actual: topic.len(),
                limit: MAX_GROUP_SESSION_TOPIC_BYTES,
            });
        }
        if let Some(topic_id) = self.owner.topics_by_name.get(&topic) {
            return Ok(*topic_id);
        }
        if let Some(topic_id) = self.staged_topics.get(&topic) {
            return Ok(*topic_id);
        }
        let actual_topics = self
            .owner
            .topics_by_name
            .len()
            .checked_add(self.staged_topics.len())
            .and_then(|count| count.checked_add(1))
            .ok_or(GroupSessionCatalogError::RetainedTopicCountOverflow)?;
        if actual_topics > MAX_GROUP_SESSION_TOPICS {
            return Err(GroupSessionCatalogError::RetainedTopicCapacity {
                actual: actual_topics,
                limit: MAX_GROUP_SESSION_TOPICS,
            });
        }
        let actual_bytes = self
            .retained_topic_name_bytes
            .checked_add(topic.len())
            .ok_or(GroupSessionCatalogError::RetainedTopicBytesOverflow)?;
        if actual_bytes > MAX_GROUP_SESSION_TOPIC_NAME_BYTES {
            return Err(GroupSessionCatalogError::RetainedTopicBytes {
                actual: actual_bytes,
                limit: MAX_GROUP_SESSION_TOPIC_NAME_BYTES,
            });
        }
        let topic_id = self
            .next_topic_id
            .ok_or(GroupSessionCatalogError::TopicIdentityExhausted)?;
        if self.owner.topics_by_id.contains_key(&topic_id) {
            return Err(GroupSessionCatalogError::TopicIdentityExhausted);
        }
        self.next_topic_id = topic_id.get().checked_add(1).map(TopicId::from_raw);
        self.retained_topic_name_bytes = actual_bytes;
        self.staged_topics.insert(topic, topic_id);
        Ok(topic_id)
    }
}
