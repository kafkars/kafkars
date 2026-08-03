//! Bounded ownership of leader-member subscriptions from `JoinGroup` responses.

use std::sync::Arc;

use kafka_client_core::{ClassicGeneration, ClassicProtocol, JoinedMemberSlot};
use kafka_wire::{
    ConsumerProtocolSubscription, decode_consumer_protocol_subscription,
    join_group_response::JoinGroupResponseMember,
};

use super::{
    ClassicJoinResponseFailure, ClassicJoinedMember, NamedAssignmentPartition,
    validation::{
        INNER_SCHEMA_VERSION, MAX_COOPERATIVE_SUBSCRIPTION_VERSION, MAX_JOIN_TOPIC_NAME_BYTES,
        MAX_MEMBER_NAME_BYTES, MAX_MEMBER_PARTITIONS, MAX_MEMBERS, MAX_TOPICS,
        subscription_decode_limits, valid_kafka_string, valid_topic,
    },
};

pub(super) fn normalize_members(
    members: &[JoinGroupResponseMember],
    local_member: &str,
    selected_version: i16,
    expected_protocol: ClassicProtocol,
) -> Result<Vec<ClassicJoinedMember>, ClassicJoinResponseFailure> {
    if members.is_empty() || members.len() > MAX_MEMBERS {
        return Err(ClassicJoinResponseFailure::MemberCount {
            actual: members.len(),
            limit: MAX_MEMBERS,
        });
    }
    let decoded = preflight_members(members, selected_version, expected_protocol)?;
    if !members
        .iter()
        .any(|member| member.member_id.as_str() == local_member)
    {
        return Err(ClassicJoinResponseFailure::LeaderMemberMissing);
    }
    let mut normalized = Vec::new();
    normalized
        .try_reserve_exact(members.len())
        .map_err(|_error| ClassicJoinResponseFailure::Allocation)?;
    for (index, (member, subscription)) in members.iter().zip(decoded).enumerate() {
        let raw_slot = u32::try_from(index + 1)
            .ok()
            .and_then(JoinedMemberSlot::try_from_raw)
            .ok_or(ClassicJoinResponseFailure::InvalidMemberSlot)?;
        let mut topics = Vec::new();
        topics
            .try_reserve_exact(subscription.topics.len())
            .map_err(|_error| ClassicJoinResponseFailure::Allocation)?;
        topics.extend(
            subscription
                .topics
                .into_iter()
                .map(|topic| Arc::from(topic.as_str())),
        );
        let generation = normalize_generation(subscription.generation_id)?;
        let owned_partitions = normalize_owned(subscription.owned_partitions)?;
        normalized.push(ClassicJoinedMember::new(
            raw_slot,
            Arc::from(member.member_id.as_str()),
            topics,
            owned_partitions,
            generation,
        ));
    }
    Ok(normalized)
}

fn preflight_members(
    members: &[JoinGroupResponseMember],
    selected_version: i16,
    expected_protocol: ClassicProtocol,
) -> Result<Vec<ConsumerProtocolSubscription>, ClassicJoinResponseFailure> {
    let mut member_bytes = 0usize;
    let mut topic_bytes = 0usize;
    let mut decoded = Vec::new();
    decoded
        .try_reserve_exact(members.len())
        .map_err(|_error| ClassicJoinResponseFailure::Allocation)?;
    for (index, member) in members.iter().enumerate() {
        validate_outer_member(members, index, selected_version)?;
        member_bytes = member_bytes
            .checked_add(member.member_id.len())
            .filter(|bytes| *bytes <= MAX_MEMBER_NAME_BYTES)
            .ok_or(ClassicJoinResponseFailure::MemberNameBytes)?;
        let subscription = decode_subscription(member.metadata.clone(), expected_protocol)?;
        for topic in &subscription.topics {
            topic_bytes = topic_bytes
                .checked_add(topic.len())
                .filter(|bytes| *bytes <= MAX_JOIN_TOPIC_NAME_BYTES)
                .ok_or(ClassicJoinResponseFailure::TopicNameBytes)?;
        }
        for owned in &subscription.owned_partitions {
            topic_bytes = topic_bytes
                .checked_add(owned.topic.len())
                .filter(|bytes| *bytes <= MAX_JOIN_TOPIC_NAME_BYTES)
                .ok_or(ClassicJoinResponseFailure::TopicNameBytes)?;
        }
        decoded.push(subscription);
    }
    Ok(decoded)
}

fn validate_outer_member(
    members: &[JoinGroupResponseMember],
    index: usize,
    selected_version: i16,
) -> Result<(), ClassicJoinResponseFailure> {
    let member = &members[index];
    if member.group_instance_id.is_some()
        && selected_version != super::validation::STATIC_JOIN_VERSION
    {
        return Err(ClassicJoinResponseFailure::StaticMember);
    }
    if member
        .group_instance_id
        .as_ref()
        .is_some_and(|identity| !valid_kafka_string(identity.as_str()))
    {
        return Err(ClassicJoinResponseFailure::StaticMember);
    }
    if !valid_kafka_string(member.member_id.as_str()) {
        return Err(ClassicJoinResponseFailure::InvalidMember);
    }
    if members[..index]
        .iter()
        .any(|prior| prior.member_id == member.member_id)
    {
        return Err(ClassicJoinResponseFailure::DuplicateMember);
    }
    Ok(())
}

fn decode_subscription(
    metadata: kafka_wire_core::Bytes,
    expected_protocol: ClassicProtocol,
) -> Result<ConsumerProtocolSubscription, ClassicJoinResponseFailure> {
    let (version, subscription) =
        decode_consumer_protocol_subscription(metadata, subscription_decode_limits())
            .map_err(ClassicJoinResponseFailure::Metadata)?;
    let version = version.value();
    let version_supported = match expected_protocol {
        ClassicProtocol::Range => version == INNER_SCHEMA_VERSION,
        ClassicProtocol::CooperativeSticky => {
            (1..=MAX_COOPERATIVE_SUBSCRIPTION_VERSION).contains(&version)
        }
    };
    if !version_supported {
        return Err(ClassicJoinResponseFailure::UnsupportedSubscriptionVersion(
            version,
        ));
    }
    if subscription.user_data.is_some() {
        return Err(ClassicJoinResponseFailure::SubscriptionUserData);
    }
    if subscription
        .rack_id
        .as_ref()
        .is_some_and(|rack_id| !valid_kafka_string(rack_id.as_str()))
    {
        return Err(ClassicJoinResponseFailure::SubscriptionRackId);
    }
    if subscription.topics.len() > MAX_TOPICS {
        return Err(ClassicJoinResponseFailure::TopicCount {
            actual: subscription.topics.len(),
            limit: MAX_TOPICS,
        });
    }
    for (index, topic) in subscription.topics.iter().enumerate() {
        if !valid_topic(topic.as_str()) {
            return Err(ClassicJoinResponseFailure::InvalidTopic);
        }
        if subscription.topics[..index]
            .iter()
            .any(|prior| prior == topic)
        {
            return Err(ClassicJoinResponseFailure::DuplicateTopic);
        }
    }
    validate_owned(&subscription)?;
    Ok(subscription)
}

fn validate_owned(
    subscription: &ConsumerProtocolSubscription,
) -> Result<(), ClassicJoinResponseFailure> {
    let actual = subscription
        .owned_partitions
        .iter()
        .try_fold(0usize, |total, owned| {
            total.checked_add(owned.partitions.len())
        })
        .ok_or(ClassicJoinResponseFailure::OwnedPartitionCount {
            actual: usize::MAX,
            limit: MAX_MEMBER_PARTITIONS,
        })?;
    if actual > MAX_MEMBER_PARTITIONS {
        return Err(ClassicJoinResponseFailure::OwnedPartitionCount {
            actual,
            limit: MAX_MEMBER_PARTITIONS,
        });
    }
    for (topic_index, owned) in subscription.owned_partitions.iter().enumerate() {
        if !valid_topic(owned.topic.as_str()) {
            return Err(ClassicJoinResponseFailure::InvalidOwnedTopic);
        }
        if topic_index > 0 {
            let prior = subscription.owned_partitions[topic_index - 1]
                .topic
                .as_str();
            if prior == owned.topic.as_str() {
                return Err(ClassicJoinResponseFailure::DuplicateOwnedTopic);
            }
            if prior > owned.topic.as_str() {
                return Err(ClassicJoinResponseFailure::OutOfOrderOwnedTopic);
            }
        }
        for (partition_index, partition) in owned.partitions.iter().copied().enumerate() {
            if partition < 0 {
                return Err(ClassicJoinResponseFailure::InvalidOwnedPartition(partition));
            }
            if partition_index > 0 {
                let prior = owned.partitions[partition_index - 1];
                if prior == partition {
                    return Err(ClassicJoinResponseFailure::DuplicateOwnedPartition);
                }
                if prior > partition {
                    return Err(ClassicJoinResponseFailure::OutOfOrderOwnedPartition);
                }
            }
        }
    }
    if subscription.generation_id < -1 {
        return Err(ClassicJoinResponseFailure::InvalidSubscriptionGeneration(
            subscription.generation_id,
        ));
    }
    Ok(())
}

fn normalize_generation(raw: i32) -> Result<Option<ClassicGeneration>, ClassicJoinResponseFailure> {
    if raw == -1 {
        return Ok(None);
    }
    ClassicGeneration::try_from_raw(raw).map(Some).ok_or(
        ClassicJoinResponseFailure::InvalidSubscriptionGeneration(raw),
    )
}

fn normalize_owned(
    owned: Vec<kafka_wire::consumer_protocol_subscription::TopicPartition>,
) -> Result<Vec<NamedAssignmentPartition>, ClassicJoinResponseFailure> {
    let count = owned.iter().map(|topic| topic.partitions.len()).sum();
    let mut normalized = Vec::new();
    normalized
        .try_reserve_exact(count)
        .map_err(|_error| ClassicJoinResponseFailure::Allocation)?;
    for owned_topic in owned {
        let topic = Arc::<str>::from(owned_topic.topic.as_str());
        normalized.extend(
            owned_topic
                .partitions
                .into_iter()
                .map(|partition| NamedAssignmentPartition::new(Arc::clone(&topic), partition)),
        );
    }
    Ok(normalized)
}
