//! Exact membership-to-broker `ShareFetch` assignment and initial-session planning.

use kafka_client_core::{AssignedTopicPartition, GroupAssignmentPartition, ShareFetchBrokerId};

use crate::protocol::consumer::share_fetch::{
    ShareFetchRequestFailure, ShareFetchRequestPlan, ShareFetchRequestTopic,
};

use super::catalog::ShareMembershipCatalog;

/// One broker-local assignment paired with its complete initial request delta.
#[must_use = "a share broker session plan must open a session or be released"]
pub(super) struct ShareBrokerSessionPlan {
    broker_id: ShareFetchBrokerId,
    assignment: Vec<AssignedTopicPartition>,
    request: ShareFetchRequestPlan,
}

impl ShareBrokerSessionPlan {
    pub(super) fn try_initial(
        catalog: &ShareMembershipCatalog,
        broker_id: ShareFetchBrokerId,
        partitions: &[GroupAssignmentPartition],
    ) -> Result<Self, ShareBrokerSessionPlanError> {
        if partitions.is_empty() {
            return Err(ShareBrokerSessionPlanError::EmptyAssignment);
        }
        let mut assignment = Vec::new();
        assignment
            .try_reserve_exact(partitions.len())
            .map_err(|_error| ShareBrokerSessionPlanError::Allocation)?;
        let mut topics: Vec<([u8; 16], Vec<u32>)> = Vec::new();
        for partition in partitions.iter().copied() {
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
                .position(|(topic_id, _partitions)| *topic_id == identity.kafka_topic_id())
            {
                index
            } else {
                topics
                    .try_reserve(1)
                    .map_err(|_error| ShareBrokerSessionPlanError::Allocation)?;
                topics.push((identity.kafka_topic_id(), Vec::new()));
                topics.len() - 1
            };
            let bucket = &mut topics[bucket_index];
            bucket
                .1
                .try_reserve(1)
                .map_err(|_error| ShareBrokerSessionPlanError::Allocation)?;
            bucket.1.push(raw_partition);
        }
        let (active, included) = materialize_initial_topics(topics)?;
        let request = ShareFetchRequestPlan::try_new(active, included, Vec::new())
            .map_err(ShareBrokerSessionPlanError::Protocol)?;
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
        ShareFetchRequestPlan,
    ) {
        (self.broker_id, self.assignment, self.request)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareBrokerSessionPlanError {
    EmptyAssignment,
    UnknownTopic,
    PartitionOutOfRange,
    DuplicatePartition,
    Allocation,
    Protocol(ShareFetchRequestFailure),
}

type TopicBucket = ([u8; 16], Vec<u32>);

fn materialize_initial_topics(
    topics: Vec<TopicBucket>,
) -> Result<(Vec<ShareFetchRequestTopic>, Vec<ShareFetchRequestTopic>), ShareBrokerSessionPlanError>
{
    let mut active = Vec::new();
    let mut included = Vec::new();
    active
        .try_reserve_exact(topics.len())
        .map_err(|_error| ShareBrokerSessionPlanError::Allocation)?;
    included
        .try_reserve_exact(topics.len())
        .map_err(|_error| ShareBrokerSessionPlanError::Allocation)?;
    for (topic_id, partitions) in topics {
        let mut included_partitions = Vec::new();
        included_partitions
            .try_reserve_exact(partitions.len())
            .map_err(|_error| ShareBrokerSessionPlanError::Allocation)?;
        included_partitions.extend_from_slice(&partitions);
        active.push(
            ShareFetchRequestTopic::try_new(topic_id, partitions)
                .map_err(ShareBrokerSessionPlanError::Protocol)?,
        );
        included.push(
            ShareFetchRequestTopic::try_new(topic_id, included_partitions)
                .map_err(ShareBrokerSessionPlanError::Protocol)?,
        );
    }
    Ok((active, included))
}
