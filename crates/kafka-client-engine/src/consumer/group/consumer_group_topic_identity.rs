//! Bounded translation between registered topic names and Kafka topic UUIDs.

use kafka_client_core::{GroupAssignmentPartition, PartitionIndex, TopicId};

use crate::{
    driver::TopicPartitionCountFact,
    protocol::consumer::{ConsumerGroupHeartbeatAssignmentTopic, ConsumerGroupHeartbeatOwnedTopic},
};

/// One registered topic paired with immutable broker identity and current bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ConsumerGroupTopicIdentity {
    local_topic_id: TopicId,
    kafka_topic_id: [u8; 16],
    logical_partition_count: u32,
}

/// Registration-time reservation failure before membership admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConsumerGroupTopicIdentityBuildError {
    Allocation,
}

/// Metadata or assignment fact outside the retained topic-identity domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConsumerGroupTopicIdentityError {
    MissingKafkaTopicId,
    ZeroPartitionCount,
    Capacity,
    DuplicateLocalTopic,
    DuplicateKafkaTopic,
    UnknownKafkaTopic,
    PartitionOutOfRange,
    Allocation,
}

/// Preallocated topic-identity table for one modern membership lifetime.
pub(super) struct ConsumerGroupTopicIdentityOwner {
    expected_topics: usize,
    identities: Vec<ConsumerGroupTopicIdentity>,
}

impl ConsumerGroupTopicIdentityOwner {
    pub(super) fn try_new(
        expected_topics: usize,
    ) -> Result<Self, ConsumerGroupTopicIdentityBuildError> {
        let mut identities = Vec::new();
        identities
            .try_reserve_exact(expected_topics)
            .map_err(|_error| ConsumerGroupTopicIdentityBuildError::Allocation)?;
        Ok(Self {
            expected_topics,
            identities,
        })
    }

    pub(super) fn next_topic(&self, local_subscription: &[TopicId]) -> Option<TopicId> {
        local_subscription.get(self.identities.len()).copied()
    }

    pub(super) fn append(
        &mut self,
        local_topic_id: TopicId,
        fact: TopicPartitionCountFact,
    ) -> Result<(), ConsumerGroupTopicIdentityError> {
        let kafka_topic_id = fact
            .kafka_topic_id
            .ok_or(ConsumerGroupTopicIdentityError::MissingKafkaTopicId)?;
        if fact.logical_partition_count == 0 {
            return Err(ConsumerGroupTopicIdentityError::ZeroPartitionCount);
        }
        if self.identities.len() == self.expected_topics {
            return Err(ConsumerGroupTopicIdentityError::Capacity);
        }
        if self
            .identities
            .iter()
            .any(|identity| identity.local_topic_id == local_topic_id)
        {
            return Err(ConsumerGroupTopicIdentityError::DuplicateLocalTopic);
        }
        if self
            .identities
            .iter()
            .any(|identity| identity.kafka_topic_id == kafka_topic_id)
        {
            return Err(ConsumerGroupTopicIdentityError::DuplicateKafkaTopic);
        }
        self.identities.push(ConsumerGroupTopicIdentity {
            local_topic_id,
            kafka_topic_id,
            logical_partition_count: fact.logical_partition_count,
        });
        Ok(())
    }

    pub(super) fn is_complete(&self) -> bool {
        self.identities.len() == self.expected_topics
    }

    pub(super) fn translate_assignment(
        &self,
        assignment: &[ConsumerGroupHeartbeatAssignmentTopic],
    ) -> Result<Vec<GroupAssignmentPartition>, ConsumerGroupTopicIdentityError> {
        let total = assignment
            .iter()
            .try_fold(0usize, |total, topic| {
                total.checked_add(topic.partitions().len())
            })
            .ok_or(ConsumerGroupTopicIdentityError::Capacity)?;
        let mut translated = Vec::new();
        translated
            .try_reserve_exact(total)
            .map_err(|_error| ConsumerGroupTopicIdentityError::Allocation)?;
        for topic in assignment {
            let identity = self
                .identities
                .iter()
                .find(|identity| identity.kafka_topic_id == topic.topic_id())
                .ok_or(ConsumerGroupTopicIdentityError::UnknownKafkaTopic)?;
            for partition in topic.partitions().iter().copied() {
                if partition >= identity.logical_partition_count {
                    return Err(ConsumerGroupTopicIdentityError::PartitionOutOfRange);
                }
                translated.push(GroupAssignmentPartition::new(
                    identity.local_topic_id,
                    PartitionIndex::from_raw(partition),
                ));
            }
        }
        translated.sort_unstable();
        Ok(translated)
    }

    pub(super) fn owned_topics(
        &self,
        assignment: &[GroupAssignmentPartition],
    ) -> Result<Vec<ConsumerGroupHeartbeatOwnedTopic>, ConsumerGroupTopicIdentityError> {
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(self.identities.len())
            .map_err(|_error| ConsumerGroupTopicIdentityError::Allocation)?;
        for identity in &self.identities {
            let mut partitions = Vec::new();
            partitions
                .try_reserve_exact(assignment.len())
                .map_err(|_error| ConsumerGroupTopicIdentityError::Allocation)?;
            partitions.extend(
                assignment
                    .iter()
                    .filter(|partition| partition.topic_id() == identity.local_topic_id)
                    .map(|partition| partition.partition().get()),
            );
            if !partitions.is_empty() {
                owned.push(ConsumerGroupHeartbeatOwnedTopic::new(
                    identity.kafka_topic_id,
                    partitions,
                ));
            }
        }
        Ok(owned)
    }
}
