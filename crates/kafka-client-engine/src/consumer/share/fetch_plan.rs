//! Exact membership-to-broker `ShareFetch` assignment and initial-session planning.

use std::sync::Arc;

use kafka_client_core::{
    AssignedTopicPartition, GroupAssignmentPartition, SHARE_FETCH_MAX_PARTITIONS_PER_BROKER,
    ShareFetchBrokerId, TopicId, partitioning::TopicMetadataGeneration,
};

use crate::protocol::consumer::share_fetch::{
    PreparedShareFetchRequest, ShareFetchRequestFailure, ShareFetchRequestPlan,
    ShareFetchRequestSettings, ShareFetchRequestTopic, share_fetch_request,
};

use super::catalog::ShareMembershipCatalog;

/// One broker-local assignment paired with its complete initial request delta.
#[must_use = "a share broker session plan must open a session or be released"]
pub(super) struct ShareBrokerSessionPlan {
    broker_id: ShareFetchBrokerId,
    assignment: Vec<AssignedTopicPartition>,
    request: ShareFetchSessionRequestPlan,
}

/// Reusable complete active set for one broker-local incremental session.
#[must_use = "a share fetch request plan must remain with its broker session"]
pub(super) struct ShareFetchSessionRequestPlan {
    topics: Vec<TopicBucket>,
}

impl ShareBrokerSessionPlan {
    pub(super) const fn broker_id(&self) -> ShareFetchBrokerId {
        self.broker_id
    }

    pub(super) fn try_initial(
        catalog: &ShareMembershipCatalog,
        broker_id: ShareFetchBrokerId,
        partitions: &[GroupAssignmentPartition],
    ) -> Result<Self, ShareBrokerSessionPlanError> {
        Self::try_from_partitions(
            catalog,
            broker_id,
            partitions.len(),
            partitions
                .iter()
                .copied()
                .map(|partition| (partition, None)),
        )
    }

    pub(super) fn try_routed(
        catalog: &ShareMembershipCatalog,
        broker_id: ShareFetchBrokerId,
        partitions: &[(GroupAssignmentPartition, TopicMetadataGeneration)],
    ) -> Result<Self, ShareBrokerSessionPlanError> {
        Self::try_from_partitions(
            catalog,
            broker_id,
            partitions.len(),
            partitions
                .iter()
                .copied()
                .map(|(partition, generation)| (partition, Some(generation))),
        )
    }

    fn try_from_partitions(
        catalog: &ShareMembershipCatalog,
        broker_id: ShareFetchBrokerId,
        partition_count: usize,
        partitions: impl Iterator<Item = (GroupAssignmentPartition, Option<TopicMetadataGeneration>)>,
    ) -> Result<Self, ShareBrokerSessionPlanError> {
        if partition_count == 0 {
            return Err(ShareBrokerSessionPlanError::EmptyAssignment);
        }
        if partition_count > SHARE_FETCH_MAX_PARTITIONS_PER_BROKER {
            return Err(ShareBrokerSessionPlanError::PartitionCapacity);
        }
        let mut assignment = Vec::new();
        assignment
            .try_reserve_exact(partition_count)
            .map_err(|_error| ShareBrokerSessionPlanError::Allocation)?;
        let mut topics: Vec<TopicBucket> = Vec::new();
        for (partition, metadata_generation) in partitions {
            let identity = catalog
                .topic_identity(partition.topic_id())
                .ok_or(ShareBrokerSessionPlanError::UnknownTopic)?;
            let raw_partition = partition.partition().get();
            if raw_partition >= identity.partition_count() {
                return Err(ShareBrokerSessionPlanError::PartitionOutOfRange);
            }
            let assigned = AssignedTopicPartition::new(partition.topic_id(), partition.partition());
            if assignment.contains(&assigned) {
                return Err(ShareBrokerSessionPlanError::DuplicatePartition);
            }
            assignment.push(assigned);
            let bucket_index = if let Some(index) = topics
                .iter()
                .position(|topic| topic.kafka_topic_id == identity.kafka_topic_id())
            {
                index
            } else {
                topics
                    .try_reserve(1)
                    .map_err(|_error| ShareBrokerSessionPlanError::Allocation)?;
                topics.push(TopicBucket {
                    local_topic_id: identity.local_topic_id(),
                    name: Arc::clone(identity.name()),
                    kafka_topic_id: identity.kafka_topic_id(),
                    metadata_generation,
                    partitions: Vec::new(),
                });
                topics.len() - 1
            };
            let bucket = &mut topics[bucket_index];
            bucket.metadata_generation = bucket.metadata_generation.max(metadata_generation);
            bucket
                .partitions
                .try_reserve(1)
                .map_err(|_error| ShareBrokerSessionPlanError::Allocation)?;
            bucket.partitions.push(raw_partition);
        }
        let request = ShareFetchSessionRequestPlan { topics };
        Ok(Self {
            broker_id,
            assignment,
            request,
        })
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        ShareFetchBrokerId,
        Vec<AssignedTopicPartition>,
        ShareFetchSessionRequestPlan,
    ) {
        (self.broker_id, self.assignment, self.request)
    }
}

impl ShareFetchSessionRequestPlan {
    pub(super) fn resolve_partition(
        &self,
        kafka_topic_id: [u8; 16],
        partition: u32,
    ) -> Option<AssignedTopicPartition> {
        self.topics
            .iter()
            .find(|topic| topic.kafka_topic_id == kafka_topic_id)
            .filter(|topic| topic.partitions.contains(&partition))
            .map(|topic| {
                AssignedTopicPartition::new(
                    topic.local_topic_id,
                    kafka_client_core::PartitionIndex::from_raw(partition),
                )
            })
    }

    pub(super) fn resolve_topic_name(&self, kafka_topic_id: [u8; 16]) -> Option<&Arc<str>> {
        self.topics
            .iter()
            .find(|topic| topic.kafka_topic_id == kafka_topic_id)
            .map(|topic| &topic.name)
    }

    pub(super) fn route_refresh_requirement(
        &self,
        kafka_topic_id: [u8; 16],
    ) -> Option<(Arc<str>, TopicMetadataGeneration)> {
        self.topics
            .iter()
            .find(|topic| topic.kafka_topic_id == kafka_topic_id)
            .and_then(|topic| {
                topic
                    .metadata_generation
                    .map(|generation| (Arc::clone(&topic.name), generation))
            })
    }

    pub(super) fn prepare(
        &self,
        group_id: &str,
        member_id: &str,
        session_epoch: i32,
        settings: ShareFetchRequestSettings,
    ) -> Result<PreparedShareFetchRequest, ShareFetchRequestFailure> {
        let active = materialize_topics(&self.topics)?;
        let included = if session_epoch == 0 {
            materialize_topics(&self.topics)?
        } else {
            Vec::new()
        };
        let plan = ShareFetchRequestPlan::try_new(active, included, Vec::new())?;
        share_fetch_request(group_id, member_id, session_epoch, settings, plan)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareBrokerSessionPlanError {
    EmptyAssignment,
    PartitionCapacity,
    UnknownTopic,
    PartitionOutOfRange,
    DuplicatePartition,
    Allocation,
    Protocol(ShareFetchRequestFailure),
}

struct TopicBucket {
    local_topic_id: TopicId,
    name: Arc<str>,
    kafka_topic_id: [u8; 16],
    metadata_generation: Option<TopicMetadataGeneration>,
    partitions: Vec<u32>,
}

fn materialize_topics(
    topics: &[TopicBucket],
) -> Result<Vec<ShareFetchRequestTopic>, ShareFetchRequestFailure> {
    let mut materialized = Vec::new();
    materialized
        .try_reserve_exact(topics.len())
        .map_err(|_error| ShareFetchRequestFailure::Allocation)?;
    for topic in topics {
        let mut request_partitions = Vec::new();
        request_partitions
            .try_reserve_exact(topic.partitions.len())
            .map_err(|_error| ShareFetchRequestFailure::Allocation)?;
        request_partitions.extend_from_slice(&topic.partitions);
        materialized.push(ShareFetchRequestTopic::try_new(
            topic.kafka_topic_id,
            request_partitions,
        )?);
    }
    Ok(materialized)
}
