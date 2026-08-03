//! Generated `JoinGroup` construction for one bounded classic subscription.

use std::sync::Arc;

use kafka_client_core::{
    ClassicGeneration, ClassicGroupTiming, ClassicProtocol, GroupAssignmentPartition, TopicId,
};
use kafka_wire::{
    ConsumerProtocolSubscription, JoinGroupRequest,
    consumer_protocol_subscription::TopicPartition as WireTopicPartition,
    encode_consumer_protocol_subscription, join_group_request::JoinGroupRequestProtocol,
};
use kafka_wire_core::{ApiVersion, BytesMut, EncodeError};

use super::ClassicSyncTopic;
use super::validation::{
    MAX_MEMBER_PARTITIONS, MAX_TOPICS, PROTOCOL_TYPE, protocol_name, subscription_version,
    valid_kafka_string, valid_topic,
};

/// Local request-shape failure before driver ownership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ClassicJoinRequestFailure {
    InvalidGroup,
    InvalidMember,
    InvalidGroupInstance,
    TopicCount { actual: usize, limit: usize },
    InvalidTopic,
    DuplicateTopic,
    OutOfOrderTopic,
    UnexpectedOwnedPartitions,
    UnexpectedGeneration,
    OwnedPartitionCount { actual: usize, limit: usize },
    DuplicateOwnedPartition,
    OutOfOrderOwnedPartition,
    OwnedPartitionOutOfRange(u32),
    OwnedTopicCount { actual: usize, limit: usize },
    InvalidOwnedTopic,
    DuplicateOwnedTopicId(TopicId),
    DuplicateOwnedTopic,
    MissingOwnedTopic(TopicId),
    Allocation,
    Encode(EncodeError),
}

/// Linear ownership of one validated generated classic Join request.
#[must_use = "a prepared classic Join request must be submitted or deliberately released"]
pub(crate) struct PreparedClassicJoinGroupRequest {
    request: JoinGroupRequest,
}

impl PreparedClassicJoinGroupRequest {
    /// Transfers the generated request at the tracked driver-call boundary.
    pub(crate) fn into_generated_join_group_request(self) -> JoinGroupRequest {
        self.request
    }

    #[cfg(test)]
    pub(super) const fn request_for_test(&self) -> &JoinGroupRequest {
        &self.request
    }
}

/// Builds one v1-v3-compatible dynamic Range `JoinGroup` request.
pub(crate) fn classic_join_group_request(
    group: &str,
    member: Option<&str>,
    topics: &[Arc<str>],
    timing: ClassicGroupTiming,
) -> Result<PreparedClassicJoinGroupRequest, ClassicJoinRequestFailure> {
    classic_join_group_request_with_instance(
        group,
        member,
        None,
        ClassicProtocol::Range,
        topics,
        &[],
        &[],
        None,
        timing,
    )
}

/// Builds one selected classic `JoinGroup` request with an optional static identity.
#[expect(
    clippy::too_many_arguments,
    reason = "one protocol request boundary preserves every classic membership and owned-partition fact"
)]
pub(crate) fn classic_join_group_request_with_instance(
    group: &str,
    member: Option<&str>,
    group_instance_id: Option<&str>,
    protocol: ClassicProtocol,
    topics: &[Arc<str>],
    owned_partitions: &[GroupAssignmentPartition],
    owned_topics: &[ClassicSyncTopic],
    generation: Option<ClassicGeneration>,
    timing: ClassicGroupTiming,
) -> Result<PreparedClassicJoinGroupRequest, ClassicJoinRequestFailure> {
    validate_inputs(
        group,
        member,
        group_instance_id,
        protocol,
        topics,
        owned_partitions,
        owned_topics,
        generation,
    )?;
    let mut subscription_topics = Vec::new();
    subscription_topics
        .try_reserve_exact(topics.len())
        .map_err(|_error| ClassicJoinRequestFailure::Allocation)?;
    subscription_topics.extend(topics.iter().map(|topic| topic.as_ref().into()));
    let mut subscription = ConsumerProtocolSubscription::default();
    subscription.topics = subscription_topics;
    if protocol == ClassicProtocol::CooperativeSticky {
        subscription.owned_partitions = materialize_owned(owned_partitions, owned_topics)?;
        subscription.generation_id = generation.map_or(-1, ClassicGeneration::get);
    }
    let mut metadata = BytesMut::new();
    encode_consumer_protocol_subscription(
        &mut metadata,
        &subscription,
        ApiVersion::new(subscription_version(protocol)),
    )
    .map_err(ClassicJoinRequestFailure::Encode)?;

    let mut protocols = Vec::new();
    protocols
        .try_reserve_exact(1)
        .map_err(|_error| ClassicJoinRequestFailure::Allocation)?;
    let mut selected = JoinGroupRequestProtocol::default();
    selected.name = protocol_name(protocol).into();
    selected.metadata = metadata.freeze();
    protocols.push(selected);

    let mut request = JoinGroupRequest::default();
    request.group_id = group.into();
    request.session_timeout_ms = timing.session_timeout_ms();
    request.rebalance_timeout_ms = timing.rebalance_timeout_ms();
    request.member_id = member.unwrap_or_default().into();
    request.group_instance_id = group_instance_id.map(Into::into);
    request.protocol_type = PROTOCOL_TYPE.into();
    request.protocols = protocols;
    request.reason = None;
    Ok(PreparedClassicJoinGroupRequest { request })
}

#[expect(
    clippy::too_many_arguments,
    reason = "validation covers the same exact classic membership and owned-partition request facts"
)]
fn validate_inputs(
    group: &str,
    member: Option<&str>,
    group_instance_id: Option<&str>,
    protocol: ClassicProtocol,
    topics: &[Arc<str>],
    owned_partitions: &[GroupAssignmentPartition],
    owned_topics: &[ClassicSyncTopic],
    generation: Option<ClassicGeneration>,
) -> Result<(), ClassicJoinRequestFailure> {
    if !valid_kafka_string(group) {
        return Err(ClassicJoinRequestFailure::InvalidGroup);
    }
    if member.is_some_and(|value| !valid_kafka_string(value)) {
        return Err(ClassicJoinRequestFailure::InvalidMember);
    }
    if group_instance_id.is_some_and(|value| !valid_kafka_string(value)) {
        return Err(ClassicJoinRequestFailure::InvalidGroupInstance);
    }
    if topics.len() > MAX_TOPICS {
        return Err(ClassicJoinRequestFailure::TopicCount {
            actual: topics.len(),
            limit: MAX_TOPICS,
        });
    }
    for (index, topic) in topics.iter().enumerate() {
        if !valid_topic(topic) {
            return Err(ClassicJoinRequestFailure::InvalidTopic);
        }
        if index > 0 && topics[index - 1].as_ref() == topic.as_ref() {
            return Err(ClassicJoinRequestFailure::DuplicateTopic);
        }
        if index > 0 && topics[index - 1].as_ref() > topic.as_ref() {
            return Err(ClassicJoinRequestFailure::OutOfOrderTopic);
        }
    }
    match protocol {
        ClassicProtocol::Range => {
            if !owned_partitions.is_empty() || !owned_topics.is_empty() {
                return Err(ClassicJoinRequestFailure::UnexpectedOwnedPartitions);
            }
            if generation.is_some() {
                return Err(ClassicJoinRequestFailure::UnexpectedGeneration);
            }
        }
        ClassicProtocol::CooperativeSticky => {
            validate_owned(owned_partitions, owned_topics)?;
        }
    }
    Ok(())
}

fn validate_owned(
    owned_partitions: &[GroupAssignmentPartition],
    owned_topics: &[ClassicSyncTopic],
) -> Result<(), ClassicJoinRequestFailure> {
    if owned_partitions.len() > MAX_MEMBER_PARTITIONS {
        return Err(ClassicJoinRequestFailure::OwnedPartitionCount {
            actual: owned_partitions.len(),
            limit: MAX_MEMBER_PARTITIONS,
        });
    }
    if owned_topics.len() > MAX_TOPICS {
        return Err(ClassicJoinRequestFailure::OwnedTopicCount {
            actual: owned_topics.len(),
            limit: MAX_TOPICS,
        });
    }
    for (index, topic) in owned_topics.iter().enumerate() {
        if !valid_topic(topic.topic()) {
            return Err(ClassicJoinRequestFailure::InvalidOwnedTopic);
        }
        if owned_topics[..index]
            .iter()
            .any(|prior| prior.topic_id() == topic.topic_id())
        {
            return Err(ClassicJoinRequestFailure::DuplicateOwnedTopicId(
                topic.topic_id(),
            ));
        }
        if owned_topics[..index]
            .iter()
            .any(|prior| prior.topic() == topic.topic())
        {
            return Err(ClassicJoinRequestFailure::DuplicateOwnedTopic);
        }
    }
    for pair in owned_partitions.windows(2) {
        if pair[0] == pair[1] {
            return Err(ClassicJoinRequestFailure::DuplicateOwnedPartition);
        }
        if pair[0] > pair[1] {
            return Err(ClassicJoinRequestFailure::OutOfOrderOwnedPartition);
        }
    }
    for partition in owned_partitions {
        let _mapped = owned_topics
            .iter()
            .find(|topic| topic.topic_id() == partition.topic_id())
            .ok_or(ClassicJoinRequestFailure::MissingOwnedTopic(
                partition.topic_id(),
            ))?;
        i32::try_from(partition.partition().get()).map_err(|_error| {
            ClassicJoinRequestFailure::OwnedPartitionOutOfRange(partition.partition().get())
        })?;
    }
    Ok(())
}

fn materialize_owned(
    partitions: &[GroupAssignmentPartition],
    topics: &[ClassicSyncTopic],
) -> Result<Vec<WireTopicPartition>, ClassicJoinRequestFailure> {
    let mut grouped = Vec::new();
    grouped
        .try_reserve_exact(partitions.len())
        .map_err(|_error| ClassicJoinRequestFailure::Allocation)?;
    let mut start = 0;
    while start < partitions.len() {
        let topic_id = partitions[start].topic_id();
        let end = partitions[start + 1..]
            .iter()
            .position(|partition| partition.topic_id() != topic_id)
            .map_or(partitions.len(), |offset| start + 1 + offset);
        let topic = topics
            .iter()
            .find(|topic| topic.topic_id() == topic_id)
            .ok_or(ClassicJoinRequestFailure::MissingOwnedTopic(topic_id))?;
        let mut wire = WireTopicPartition::default();
        wire.topic = topic.topic().into();
        wire.partitions
            .try_reserve_exact(end - start)
            .map_err(|_error| ClassicJoinRequestFailure::Allocation)?;
        for partition in &partitions[start..end] {
            wire.partitions.push(
                i32::try_from(partition.partition().get()).map_err(|_error| {
                    ClassicJoinRequestFailure::OwnedPartitionOutOfRange(partition.partition().get())
                })?,
            );
        }
        grouped.push(wire);
        start = end;
    }
    Ok(grouped)
}
