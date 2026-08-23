//! Bounded mapping from share subscriptions to local and broker topic identity.

use std::sync::Arc;

use kafka_client_core::{GroupAssignmentPartition, PartitionIndex, TopicId};

use crate::protocol::consumer::share_group::ShareGroupHeartbeatAssignmentTopic;

/// One immutable subscription topic resolved before membership starts.
pub(in crate::consumer) struct ShareTopicIdentity {
    local_topic_id: TopicId,
    name: Arc<str>,
    kafka_topic_id: [u8; 16],
    partition_count: u32,
}

impl ShareTopicIdentity {
    pub(super) const fn new(
        local_topic_id: TopicId,
        name: Arc<str>,
        kafka_topic_id: [u8; 16],
        partition_count: u32,
    ) -> Self {
        Self {
            local_topic_id,
            name,
            kafka_topic_id,
            partition_count,
        }
    }

    pub(super) const fn local_topic_id(&self) -> TopicId {
        self.local_topic_id
    }

    pub(super) fn name(&self) -> &Arc<str> {
        &self.name
    }

    pub(super) const fn kafka_topic_id(&self) -> [u8; 16] {
        self.kafka_topic_id
    }
}

/// Prevalidated spellings and topic identities for one share member.
pub(in crate::consumer) struct ShareMembershipCatalog {
    group: Arc<str>,
    member: Arc<str>,
    rack: Option<Arc<str>>,
    topics: Vec<ShareTopicIdentity>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum ShareMembershipCatalogError {
    EmptyGroup,
    EmptyMember,
    EmptySubscription,
    InvalidTopic,
    ZeroTopicIdentity,
    ZeroPartitionCount,
    DuplicateLocalTopic,
    DuplicateTopicName,
    DuplicateKafkaTopic,
    UnknownKafkaTopic,
    PartitionOutOfRange,
    Allocation,
}

impl ShareMembershipCatalog {
    pub(super) fn try_new(
        group: Arc<str>,
        member: Arc<str>,
        rack: Option<Arc<str>>,
        topics: Vec<ShareTopicIdentity>,
    ) -> Result<Self, ShareMembershipCatalogError> {
        if group.is_empty() {
            return Err(ShareMembershipCatalogError::EmptyGroup);
        }
        if member.is_empty() {
            return Err(ShareMembershipCatalogError::EmptyMember);
        }
        if topics.is_empty() {
            return Err(ShareMembershipCatalogError::EmptySubscription);
        }
        validate_topics(&topics)?;
        Ok(Self {
            group,
            member,
            rack,
            topics,
        })
    }

    pub(super) fn group(&self) -> &str {
        &self.group
    }

    pub(super) fn member(&self) -> &str {
        &self.member
    }

    pub(super) fn rack(&self) -> Option<&str> {
        self.rack.as_deref()
    }

    pub(super) fn topic_names(&self) -> impl Iterator<Item = &str> {
        self.topics.iter().map(|topic| topic.name.as_ref())
    }

    pub(super) fn topic_name(&self, topic_id: TopicId) -> Option<&Arc<str>> {
        self.topics
            .iter()
            .find(|topic| topic.local_topic_id == topic_id)
            .map(ShareTopicIdentity::name)
    }

    pub(super) fn translate_assignment(
        &self,
        source: &[ShareGroupHeartbeatAssignmentTopic],
    ) -> Result<Vec<GroupAssignmentPartition>, ShareMembershipCatalogError> {
        let total = source.iter().try_fold(0usize, |total, topic| {
            total.checked_add(topic.partitions().len())
        });
        let mut translated = Vec::new();
        translated
            .try_reserve_exact(total.ok_or(ShareMembershipCatalogError::Allocation)?)
            .map_err(|_error| ShareMembershipCatalogError::Allocation)?;
        for source_topic in source {
            let topic = self
                .topics
                .iter()
                .find(|topic| topic.kafka_topic_id == source_topic.topic_id())
                .ok_or(ShareMembershipCatalogError::UnknownKafkaTopic)?;
            for partition in source_topic.partitions().iter().copied() {
                if partition >= topic.partition_count {
                    return Err(ShareMembershipCatalogError::PartitionOutOfRange);
                }
                translated.push(GroupAssignmentPartition::new(
                    topic.local_topic_id,
                    PartitionIndex::from_raw(partition),
                ));
            }
        }
        translated.sort_unstable();
        Ok(translated)
    }
}

fn validate_topics(topics: &[ShareTopicIdentity]) -> Result<(), ShareMembershipCatalogError> {
    for (index, topic) in topics.iter().enumerate() {
        if topic.name.is_empty() {
            return Err(ShareMembershipCatalogError::InvalidTopic);
        }
        if topic.kafka_topic_id == [0; 16] {
            return Err(ShareMembershipCatalogError::ZeroTopicIdentity);
        }
        if topic.partition_count == 0 {
            return Err(ShareMembershipCatalogError::ZeroPartitionCount);
        }
        let preceding = &topics[..index];
        if preceding
            .iter()
            .any(|candidate| candidate.local_topic_id == topic.local_topic_id)
        {
            return Err(ShareMembershipCatalogError::DuplicateLocalTopic);
        }
        if preceding
            .iter()
            .any(|candidate| candidate.name == topic.name)
        {
            return Err(ShareMembershipCatalogError::DuplicateTopicName);
        }
        if preceding
            .iter()
            .any(|candidate| candidate.kafka_topic_id == topic.kafka_topic_id)
        {
            return Err(ShareMembershipCatalogError::DuplicateKafkaTopic);
        }
    }
    Ok(())
}
