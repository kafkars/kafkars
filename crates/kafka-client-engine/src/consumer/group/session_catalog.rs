//! Sole ownership of durable topic spellings and committed assignment facts.

use std::{collections::BTreeMap, sync::Arc};

use kafka_client_core::{GroupId, LiveGroupAssignment, MemberId, MembershipCycle, TopicId};

use super::classic_group_event::ClassicGroupEventStore;
use super::session_catalog_consumer::ConsumerGroupSession;

mod static_membership;
#[cfg(test)]
mod static_membership_test;

pub(super) use static_membership::RequiredJoinMember;

pub(super) const MAX_GROUP_SESSION_TOPICS: usize = 64;
pub(super) const MAX_GROUP_SESSION_TOPIC_BYTES: usize = 249;
pub(super) const MAX_GROUP_SESSION_TOPIC_NAME_BYTES: usize = 16 * 1024;
pub(super) const MAX_KAFKA_GROUP_STRING_BYTES: usize = i16::MAX as usize;

pub(super) struct CurrentGroupSession {
    pub(super) member_id: MemberId,
    pub(super) member: Arc<str>,
    pub(super) installed_cycle: MembershipCycle,
    pub(super) classic_generation: i32,
    pub(super) assignment: LiveGroupAssignment,
}

/// Bounded staging or lookup failure that leaves the current session intact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupSessionCatalogError {
    EmptyGroup,
    GroupBytes { actual: usize, limit: usize },
    EmptyGroupInstance,
    GroupInstanceBytes { actual: usize, limit: usize },
    EmptyMember,
    MemberBytes { actual: usize, limit: usize },
    EmptyTopic,
    TopicBytes { actual: usize, limit: usize },
    RetainedTopicCapacity { actual: usize, limit: usize },
    RetainedTopicBytes { actual: usize, limit: usize },
    RetainedTopicBytesOverflow,
    DuplicateTopic,
    TopicIdentityExhausted,
    Allocation,
    UnknownTopic(TopicId),
    MemberMismatch,
    SessionProtocolMismatch,
}

/// One fixed group identity, persistent topic catalog, and current member.
pub(super) struct GroupSessionCatalog {
    group_id: GroupId,
    group: Arc<str>,
    group_instance_id: Option<Arc<str>>,
    pub(super) next_member_id: Option<MemberId>,
    pub(super) next_topic_id: Option<TopicId>,
    pub(super) retained_topic_name_bytes: usize,
    pub(super) topics_by_name: BTreeMap<Arc<str>, TopicId>,
    pub(super) topics_by_id: BTreeMap<TopicId, Arc<str>>,
    local_subscription: Vec<TopicId>,
    pub(super) current: Option<CurrentGroupSession>,
    pub(super) consumer_current: Option<ConsumerGroupSession>,
    pub(super) required_join_member: Option<RequiredJoinMember>,
    pub(super) events: ClassicGroupEventStore,
}

impl GroupSessionCatalog {
    pub(super) fn try_new(
        group_id: GroupId,
        group: Arc<str>,
        local_topics: &[Arc<str>],
    ) -> Result<Self, GroupSessionCatalogError> {
        validate_kafka_string(&group, GroupSessionCatalogError::EmptyGroup, |actual| {
            GroupSessionCatalogError::GroupBytes {
                actual,
                limit: MAX_KAFKA_GROUP_STRING_BYTES,
            }
        })?;
        if local_topics.len() > MAX_GROUP_SESSION_TOPICS {
            return Err(GroupSessionCatalogError::RetainedTopicCapacity {
                actual: local_topics.len(),
                limit: MAX_GROUP_SESSION_TOPICS,
            });
        }
        let mut ordered = Vec::new();
        ordered
            .try_reserve_exact(local_topics.len())
            .map_err(|_error| GroupSessionCatalogError::Allocation)?;
        ordered.extend(local_topics.iter().cloned());
        ordered.sort_unstable_by(|left, right| left.as_ref().cmp(right.as_ref()));
        if ordered.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(GroupSessionCatalogError::DuplicateTopic);
        }
        let mut topics_by_name = BTreeMap::new();
        let mut topics_by_id = BTreeMap::new();
        let mut local_subscription = Vec::new();
        local_subscription
            .try_reserve_exact(ordered.len())
            .map_err(|_error| GroupSessionCatalogError::Allocation)?;
        let mut next_topic_id = Some(TopicId::from_raw(1));
        let mut retained_topic_name_bytes = 0usize;
        for topic in ordered {
            validate_topic(&topic)?;
            retained_topic_name_bytes = retained_topic_name_bytes
                .checked_add(topic.len())
                .ok_or(GroupSessionCatalogError::RetainedTopicBytesOverflow)?;
            if retained_topic_name_bytes > MAX_GROUP_SESSION_TOPIC_NAME_BYTES {
                return Err(GroupSessionCatalogError::RetainedTopicBytes {
                    actual: retained_topic_name_bytes,
                    limit: MAX_GROUP_SESSION_TOPIC_NAME_BYTES,
                });
            }
            let topic_id = next_topic_id.ok_or(GroupSessionCatalogError::TopicIdentityExhausted)?;
            next_topic_id = topic_id.get().checked_add(1).map(TopicId::from_raw);
            topics_by_name.insert(Arc::clone(&topic), topic_id);
            topics_by_id.insert(topic_id, topic);
            local_subscription.push(topic_id);
        }
        Ok(Self {
            group_id,
            group,
            group_instance_id: None,
            next_member_id: MemberId::try_from_raw(1),
            next_topic_id,
            retained_topic_name_bytes,
            topics_by_name,
            topics_by_id,
            local_subscription,
            current: None,
            consumer_current: None,
            required_join_member: None,
            events: ClassicGroupEventStore::new(),
        })
    }

    pub(super) const fn group_id(&self) -> GroupId {
        self.group_id
    }

    pub(super) fn group(&self) -> &Arc<str> {
        &self.group
    }

    pub(super) fn current_member_id(&self) -> Option<MemberId> {
        self.current
            .as_ref()
            .map(|current| current.member_id)
            .or_else(|| {
                self.consumer_current
                    .as_ref()
                    .map(ConsumerGroupSession::member_id)
            })
    }

    pub(super) fn current_member(&self) -> Option<&Arc<str>> {
        self.current
            .as_ref()
            .map(|current| &current.member)
            .or_else(|| {
                self.consumer_current
                    .as_ref()
                    .map(ConsumerGroupSession::member)
            })
    }

    pub(super) fn classic_generation(&self) -> Option<i32> {
        self.current
            .as_ref()
            .map(|current| current.classic_generation)
    }

    pub(super) fn membership_cycle(&self) -> Option<MembershipCycle> {
        self.current
            .as_ref()
            .map(|current| current.installed_cycle)
            .or_else(|| {
                self.consumer_current
                    .as_ref()
                    .map(ConsumerGroupSession::installed_cycle)
            })
    }

    pub(super) fn live_assignment(&self) -> Option<&LiveGroupAssignment> {
        self.current
            .as_ref()
            .map(|current| &current.assignment)
            .or_else(|| {
                self.consumer_current
                    .as_ref()
                    .and_then(ConsumerGroupSession::assignment)
            })
    }

    pub(super) fn topic_name(
        &self,
        topic_id: TopicId,
    ) -> Result<&Arc<str>, GroupSessionCatalogError> {
        self.topics_by_id
            .get(&topic_id)
            .ok_or(GroupSessionCatalogError::UnknownTopic(topic_id))
    }

    pub(super) fn copy_topic_name(
        &self,
        topic_id: TopicId,
    ) -> Result<String, GroupSessionCatalogError> {
        let topic = self.topic_name(topic_id)?;
        let mut copied = String::new();
        copied
            .try_reserve_exact(topic.len())
            .map_err(|_error| GroupSessionCatalogError::Allocation)?;
        copied.push_str(topic);
        Ok(copied)
    }

    pub(super) fn topic_id(&self, topic: &str) -> Option<TopicId> {
        self.topics_by_name.get(topic).copied()
    }

    pub(super) fn local_subscription(&self) -> &[TopicId] {
        &self.local_subscription
    }

    pub(super) fn retained_topic_count(&self) -> usize {
        self.topics_by_id.len()
    }

    pub(super) const fn retained_topic_name_bytes(&self) -> usize {
        self.retained_topic_name_bytes
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

pub(super) fn validate_topic(topic: &str) -> Result<(), GroupSessionCatalogError> {
    if topic.is_empty() {
        return Err(GroupSessionCatalogError::EmptyTopic);
    }
    if topic.len() > MAX_GROUP_SESSION_TOPIC_BYTES {
        return Err(GroupSessionCatalogError::TopicBytes {
            actual: topic.len(),
            limit: MAX_GROUP_SESSION_TOPIC_BYTES,
        });
    }
    Ok(())
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
