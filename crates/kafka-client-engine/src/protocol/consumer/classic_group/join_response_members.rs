//! Bounded ownership of leader-member subscriptions from `JoinGroup` responses.

use std::sync::Arc;

use kafka_client_core::JoinedMemberSlot;
use kafka_wire::{
    ConsumerProtocolSubscription, decode_consumer_protocol_subscription,
    join_group_response::JoinGroupResponseMember,
};

use super::{
    ClassicJoinResponseFailure, ClassicJoinedMember,
    validation::{
        INNER_SCHEMA_VERSION, MAX_JOIN_TOPIC_NAME_BYTES, MAX_MEMBER_NAME_BYTES, MAX_MEMBERS,
        MAX_TOPICS, inner_decode_limits, valid_kafka_string, valid_topic,
    },
};

pub(super) fn normalize_members(
    members: &[JoinGroupResponseMember],
    local_member: &str,
    selected_version: i16,
) -> Result<Vec<ClassicJoinedMember>, ClassicJoinResponseFailure> {
    if members.is_empty() || members.len() > MAX_MEMBERS {
        return Err(ClassicJoinResponseFailure::MemberCount {
            actual: members.len(),
            limit: MAX_MEMBERS,
        });
    }
    let decoded = preflight_members(members, selected_version)?;
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
        normalized.push(ClassicJoinedMember::new(
            raw_slot,
            Arc::from(member.member_id.as_str()),
            topics,
        ));
    }
    Ok(normalized)
}

fn preflight_members(
    members: &[JoinGroupResponseMember],
    selected_version: i16,
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
        let subscription = decode_subscription(member.metadata.clone())?;
        for topic in &subscription.topics {
            topic_bytes = topic_bytes
                .checked_add(topic.len())
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
) -> Result<ConsumerProtocolSubscription, ClassicJoinResponseFailure> {
    let (version, subscription) =
        decode_consumer_protocol_subscription(metadata, inner_decode_limits())
            .map_err(ClassicJoinResponseFailure::Metadata)?;
    if version.value() != INNER_SCHEMA_VERSION {
        return Err(ClassicJoinResponseFailure::UnsupportedSubscriptionVersion(
            version.value(),
        ));
    }
    if subscription.user_data.is_some() {
        return Err(ClassicJoinResponseFailure::SubscriptionUserData);
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
    Ok(subscription)
}
