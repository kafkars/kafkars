//! Explicit API-77 scalar, count, text, diagnostic, and retained-byte bounds.

use core::mem::size_of;

use kafka_client_core::{
    DESCRIBE_SHARE_GROUP_DIAGNOSTIC_BYTES, DESCRIBE_SHARE_GROUP_MAX_ASSIGNMENT_TOPICS,
    DESCRIBE_SHARE_GROUP_MAX_MEMBERS, DESCRIBE_SHARE_GROUP_MAX_PARTITIONS_PER_TOPIC,
    DESCRIBE_SHARE_GROUP_MAX_RESPONSE_TEXT_BYTES, DESCRIBE_SHARE_GROUP_MAX_SCALAR_BYTES,
    DESCRIBE_SHARE_GROUP_MAX_SUBSCRIPTIONS,
};
use kafka_wire::share_group_describe_response::{DescribedGroup, Member, TopicPartitions};

use super::{
    DescribeShareGroupAssignment, DescribeShareGroupBrokerError, DescribeShareGroupDescription,
    DescribeShareGroupMember, DescribeShareGroupProtocolFailure, DescribeShareGroupResult,
    DescribeShareGroupTopicPartitions, NormalizedDescribeShareGroupResponse,
};

pub(super) const MAX_DIAGNOSTIC_BYTES: usize = DESCRIBE_SHARE_GROUP_DIAGNOSTIC_BYTES;
pub(super) const MAX_SCALAR_BYTES: usize = DESCRIBE_SHARE_GROUP_MAX_SCALAR_BYTES;
pub(super) const MAX_RESPONSE_TEXT_BYTES: usize = DESCRIBE_SHARE_GROUP_MAX_RESPONSE_TEXT_BYTES;
pub(super) const MAX_MEMBERS: usize = DESCRIBE_SHARE_GROUP_MAX_MEMBERS;
pub(super) const MAX_SUBSCRIPTIONS_PER_MEMBER: usize = DESCRIBE_SHARE_GROUP_MAX_SUBSCRIPTIONS;
pub(super) const MAX_ASSIGNMENT_TOPICS_PER_MEMBER: usize =
    DESCRIBE_SHARE_GROUP_MAX_ASSIGNMENT_TOPICS;
pub(super) const MAX_PARTITIONS_PER_TOPIC: usize = DESCRIBE_SHARE_GROUP_MAX_PARTITIONS_PER_TOPIC;
pub(super) const MAX_RESPONSE_PARTITIONS: usize = DESCRIBE_SHARE_GROUP_MAX_PARTITIONS_PER_TOPIC;

pub(super) fn success_required_bytes(
    group: &DescribedGroup,
) -> Result<usize, DescribeShareGroupProtocolFailure> {
    let mut bytes = size_of::<NormalizedDescribeShareGroupResponse>()
        .checked_add(group.group_id.len())
        .and_then(|value| value.checked_add(size_of::<DescribeShareGroupResult>()))
        .and_then(|value| value.checked_add(size_of::<DescribeShareGroupDescription>()))
        .and_then(|value| value.checked_add(group.group_state.len()))
        .and_then(|value| value.checked_add(group.assignor_name.len()))
        .ok_or(DescribeShareGroupProtocolFailure::RetainedBytesOverflow)?;
    for member in &group.members {
        bytes = charge_member(bytes, member)?;
    }
    Ok(bytes)
}

fn charge_member(
    mut bytes: usize,
    member: &Member,
) -> Result<usize, DescribeShareGroupProtocolFailure> {
    bytes = bytes
        .checked_add(size_of::<DescribeShareGroupMember>())
        .and_then(|value| value.checked_add(member.member_id.len()))
        .and_then(|value| {
            value.checked_add(
                member
                    .rack_id
                    .as_ref()
                    .map_or(0, kafka_wire_core::StrBytes::len),
            )
        })
        .and_then(|value| value.checked_add(member.client_id.len()))
        .and_then(|value| value.checked_add(member.client_host.len()))
        .and_then(|value| value.checked_add(size_of::<DescribeShareGroupAssignment>()))
        .ok_or(DescribeShareGroupProtocolFailure::RetainedBytesOverflow)?;
    bytes = member
        .subscribed_topic_names
        .len()
        .checked_mul(size_of::<String>())
        .and_then(|items| bytes.checked_add(items))
        .ok_or(DescribeShareGroupProtocolFailure::RetainedBytesOverflow)?;
    for topic in &member.subscribed_topic_names {
        bytes = bytes
            .checked_add(topic.len())
            .ok_or(DescribeShareGroupProtocolFailure::RetainedBytesOverflow)?;
    }
    for topic in &member.assignment.topic_partitions {
        bytes = charge_topic(bytes, topic)?;
    }
    Ok(bytes)
}

fn charge_topic(
    bytes: usize,
    topic: &TopicPartitions,
) -> Result<usize, DescribeShareGroupProtocolFailure> {
    bytes
        .checked_add(size_of::<DescribeShareGroupTopicPartitions>())
        .and_then(|value| value.checked_add(topic.topic_name.len()))
        .and_then(|value| {
            topic
                .partitions
                .len()
                .checked_mul(size_of::<i32>())
                .and_then(|items| value.checked_add(items))
        })
        .ok_or(DescribeShareGroupProtocolFailure::RetainedBytesOverflow)
}

pub(super) fn error_required_bytes(
    group: &DescribedGroup,
) -> Result<usize, DescribeShareGroupProtocolFailure> {
    size_of::<NormalizedDescribeShareGroupResponse>()
        .checked_add(group.group_id.len())
        .and_then(|value| value.checked_add(size_of::<DescribeShareGroupResult>()))
        .and_then(|value| value.checked_add(size_of::<DescribeShareGroupBrokerError>()))
        .and_then(|value| {
            value.checked_add(
                group
                    .error_message
                    .as_ref()
                    .map_or(0, |message| message.len().min(MAX_DIAGNOSTIC_BYTES)),
            )
        })
        .ok_or(DescribeShareGroupProtocolFailure::RetainedBytesOverflow)
}

pub(super) fn scratch_required_bytes(
    group: &DescribedGroup,
) -> Result<usize, DescribeShareGroupProtocolFailure> {
    let mut bytes = group
        .members
        .len()
        .checked_mul(size_of::<&Member>())
        .ok_or(DescribeShareGroupProtocolFailure::RetainedBytesOverflow)?;
    for member in &group.members {
        bytes = member
            .subscribed_topic_names
            .len()
            .checked_mul(size_of::<&kafka_wire_core::StrBytes>())
            .and_then(|value| bytes.checked_add(value))
            .ok_or(DescribeShareGroupProtocolFailure::RetainedBytesOverflow)?;
        bytes = member
            .assignment
            .topic_partitions
            .len()
            .checked_mul(size_of::<&TopicPartitions>())
            .and_then(|value| bytes.checked_add(value))
            .ok_or(DescribeShareGroupProtocolFailure::RetainedBytesOverflow)?;
        for topic in &member.assignment.topic_partitions {
            bytes = topic
                .partitions
                .len()
                .checked_mul(size_of::<i32>())
                .and_then(|value| bytes.checked_add(value))
                .ok_or(DescribeShareGroupProtocolFailure::RetainedBytesOverflow)?;
        }
    }
    Ok(bytes)
}

pub(super) fn bounded_diagnostic(
    message: Option<&str>,
) -> Result<(Option<String>, bool), DescribeShareGroupProtocolFailure> {
    let Some(message) = message else {
        return Ok((None, false));
    };
    let mut end = message.len().min(MAX_DIAGNOSTIC_BYTES);
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    Ok((Some(clone_string(&message[..end])?), end < message.len()))
}

pub(super) fn clone_string(value: &str) -> Result<String, DescribeShareGroupProtocolFailure> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_| DescribeShareGroupProtocolFailure::Allocation)?;
    owned.push_str(value);
    Ok(owned)
}
