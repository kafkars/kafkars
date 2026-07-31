//! Strict API 68 v0 request construction for join, steady heartbeat, and leave.

use kafka_wire::{
    ConsumerGroupHeartbeatRequest, consumer_group_heartbeat_request::TopicPartitions,
};
use kafka_wire_core::Uuid;

use super::model::{
    CONSUMER_GROUP_HEARTBEAT_MAX_PARTITIONS, CONSUMER_GROUP_HEARTBEAT_MAX_TOPIC_BYTES,
    CONSUMER_GROUP_HEARTBEAT_MAX_TOPICS, ConsumerGroupHeartbeatOwnedTopic, MAX_KAFKA_STRING_BYTES,
};

/// Local request-shape failure before generated or driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConsumerGroupHeartbeatRequestFailure {
    GroupId,
    MemberId,
    MemberEpoch(i32),
    InstanceId,
    RebalanceTimeout(u32),
    EmptySubscription,
    TooManyTopics { actual: usize, limit: usize },
    TopicName,
    DuplicateTopicName,
    ZeroTopicId,
    EmptyTopicPartitions,
    DuplicateTopicId,
    DuplicatePartition(u32),
    PartitionOutOfRange(u32),
    TooManyPartitions { actual: usize, limit: usize },
    Allocation,
}

/// Linear ownership of one validated generated API 68 request.
#[must_use = "a prepared ConsumerGroupHeartbeat request must be submitted or released"]
pub(crate) struct PreparedConsumerGroupHeartbeatRequest {
    request: ConsumerGroupHeartbeatRequest,
}

impl PreparedConsumerGroupHeartbeatRequest {
    /// Transfers the generated request at the tracked driver-call boundary.
    pub(crate) fn into_generated_request(self) -> ConsumerGroupHeartbeatRequest {
        self.request
    }

    #[cfg(test)]
    pub(super) const fn request_for_test(&self) -> &ConsumerGroupHeartbeatRequest {
        &self.request
    }
}

/// Builds the initial epoch-zero API 68 v0 request with the complete subscription.
pub(crate) fn consumer_group_join_request(
    group_id: &str,
    instance_id: Option<&str>,
    rebalance_timeout_ms: u32,
    topics: &[&str],
) -> Result<PreparedConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatRequestFailure> {
    validate_group(group_id)?;
    if instance_id.is_some_and(|value| !valid_kafka_string(value)) {
        return Err(ConsumerGroupHeartbeatRequestFailure::InstanceId);
    }
    let rebalance_timeout_ms = i32::try_from(rebalance_timeout_ms).map_err(|_overflow| {
        ConsumerGroupHeartbeatRequestFailure::RebalanceTimeout(rebalance_timeout_ms)
    })?;
    let subscribed_topic_names = subscription(topics)?;
    let mut request = ConsumerGroupHeartbeatRequest::default();
    request.group_id = group_id.into();
    request.member_id = "".into();
    request.member_epoch = 0;
    request.instance_id = instance_id.map(Into::into);
    request.rebalance_timeout_ms = rebalance_timeout_ms;
    request.subscribed_topic_names = Some(subscribed_topic_names);
    Ok(PreparedConsumerGroupHeartbeatRequest { request })
}

/// Builds one positive-epoch API 68 v0 heartbeat with explicit current ownership.
pub(crate) fn consumer_group_steady_request(
    group_id: &str,
    member_id: &str,
    member_epoch: i32,
    owned_topics: Option<&[ConsumerGroupHeartbeatOwnedTopic]>,
) -> Result<PreparedConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatRequestFailure> {
    validate_group_and_member(group_id, member_id)?;
    if member_epoch <= 0 {
        return Err(ConsumerGroupHeartbeatRequestFailure::MemberEpoch(
            member_epoch,
        ));
    }
    let mut request = ConsumerGroupHeartbeatRequest::default();
    request.group_id = group_id.into();
    request.member_id = member_id.into();
    request.member_epoch = member_epoch;
    request.topic_partitions = owned_topics.map(normalize_owned_topics).transpose()?;
    Ok(PreparedConsumerGroupHeartbeatRequest { request })
}

/// Builds the epoch-minus-one API 68 v0 leave request.
pub(crate) fn consumer_group_leave_request(
    group_id: &str,
    member_id: &str,
) -> Result<PreparedConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatRequestFailure> {
    validate_group_and_member(group_id, member_id)?;
    let mut request = ConsumerGroupHeartbeatRequest::default();
    request.group_id = group_id.into();
    request.member_id = member_id.into();
    request.member_epoch = -1;
    Ok(PreparedConsumerGroupHeartbeatRequest { request })
}

fn subscription(
    topics: &[&str],
) -> Result<Vec<kafka_wire_core::StrBytes>, ConsumerGroupHeartbeatRequestFailure> {
    if topics.is_empty() {
        return Err(ConsumerGroupHeartbeatRequestFailure::EmptySubscription);
    }
    if topics.len() > CONSUMER_GROUP_HEARTBEAT_MAX_TOPICS {
        return Err(ConsumerGroupHeartbeatRequestFailure::TooManyTopics {
            actual: topics.len(),
            limit: CONSUMER_GROUP_HEARTBEAT_MAX_TOPICS,
        });
    }
    let mut retained = Vec::new();
    retained
        .try_reserve_exact(topics.len())
        .map_err(|_allocation| ConsumerGroupHeartbeatRequestFailure::Allocation)?;
    for (index, topic) in topics.iter().enumerate() {
        if topic.is_empty() || topic.len() > CONSUMER_GROUP_HEARTBEAT_MAX_TOPIC_BYTES {
            return Err(ConsumerGroupHeartbeatRequestFailure::TopicName);
        }
        if topics[..index].contains(topic) {
            return Err(ConsumerGroupHeartbeatRequestFailure::DuplicateTopicName);
        }
        retained.push((*topic).into());
    }
    Ok(retained)
}

fn normalize_owned_topics(
    topics: &[ConsumerGroupHeartbeatOwnedTopic],
) -> Result<Vec<TopicPartitions>, ConsumerGroupHeartbeatRequestFailure> {
    if topics.len() > CONSUMER_GROUP_HEARTBEAT_MAX_TOPICS {
        return Err(ConsumerGroupHeartbeatRequestFailure::TooManyTopics {
            actual: topics.len(),
            limit: CONSUMER_GROUP_HEARTBEAT_MAX_TOPICS,
        });
    }
    let total = topics.iter().try_fold(0usize, |total, topic| {
        total.checked_add(topic.partitions().len())
    });
    let Some(total) = total else {
        return Err(ConsumerGroupHeartbeatRequestFailure::TooManyPartitions {
            actual: usize::MAX,
            limit: CONSUMER_GROUP_HEARTBEAT_MAX_PARTITIONS,
        });
    };
    if total > CONSUMER_GROUP_HEARTBEAT_MAX_PARTITIONS {
        return Err(ConsumerGroupHeartbeatRequestFailure::TooManyPartitions {
            actual: total,
            limit: CONSUMER_GROUP_HEARTBEAT_MAX_PARTITIONS,
        });
    }
    let mut generated = Vec::new();
    generated
        .try_reserve_exact(topics.len())
        .map_err(|_allocation| ConsumerGroupHeartbeatRequestFailure::Allocation)?;
    for (index, topic) in topics.iter().enumerate() {
        validate_owned_topic(&topics[..index], topic)?;
        let mut partitions = Vec::new();
        partitions
            .try_reserve_exact(topic.partitions().len())
            .map_err(|_allocation| ConsumerGroupHeartbeatRequestFailure::Allocation)?;
        for (partition_index, partition) in topic.partitions().iter().copied().enumerate() {
            if topic.partitions()[..partition_index].contains(&partition) {
                return Err(ConsumerGroupHeartbeatRequestFailure::DuplicatePartition(
                    partition,
                ));
            }
            partitions.push(i32::try_from(partition).map_err(|_overflow| {
                ConsumerGroupHeartbeatRequestFailure::PartitionOutOfRange(partition)
            })?);
        }
        partitions.sort_unstable();
        let mut generated_topic = TopicPartitions::default();
        generated_topic.topic_id = Uuid::from_bytes(topic.topic_id());
        generated_topic.partitions = partitions;
        generated.push(generated_topic);
    }
    generated.sort_unstable_by_key(|topic| topic.topic_id);
    Ok(generated)
}

fn validate_owned_topic(
    previous: &[ConsumerGroupHeartbeatOwnedTopic],
    topic: &ConsumerGroupHeartbeatOwnedTopic,
) -> Result<(), ConsumerGroupHeartbeatRequestFailure> {
    if topic.topic_id() == [0; 16] {
        return Err(ConsumerGroupHeartbeatRequestFailure::ZeroTopicId);
    }
    if topic.partitions().is_empty() {
        return Err(ConsumerGroupHeartbeatRequestFailure::EmptyTopicPartitions);
    }
    if previous
        .iter()
        .any(|candidate| candidate.topic_id() == topic.topic_id())
    {
        return Err(ConsumerGroupHeartbeatRequestFailure::DuplicateTopicId);
    }
    Ok(())
}

fn validate_group_and_member(
    group_id: &str,
    member_id: &str,
) -> Result<(), ConsumerGroupHeartbeatRequestFailure> {
    validate_group(group_id)?;
    if !valid_kafka_string(member_id) {
        return Err(ConsumerGroupHeartbeatRequestFailure::MemberId);
    }
    Ok(())
}

fn validate_group(group_id: &str) -> Result<(), ConsumerGroupHeartbeatRequestFailure> {
    if !valid_kafka_string(group_id) {
        return Err(ConsumerGroupHeartbeatRequestFailure::GroupId);
    }
    Ok(())
}

fn valid_kafka_string(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_KAFKA_STRING_BYTES
}
