//! Charged bounded-sort shape and retained-byte validation for API-key 69 responses.

use core::mem::size_of;

use kafka_wire::consumer_group_describe_response::{
    Assignment, DescribedGroup, Member, TopicPartitions,
};

use super::{
    modern_assignment::ConsumerGroupDescribeTopicPartitions,
    modern_model::{ConsumerGroupDescribeDescription, ConsumerGroupDescribeMember},
    modern_outcome::NormalizedConsumerGroupDescribeResponse,
    modern_response::ConsumerGroupDescribeResponseFailure,
    modern_response_duplicates::{
        validate_unique_members, validate_unique_partitions, validate_unique_subscriptions,
        validate_unique_topics,
    },
};

const MAX_DIAGNOSTIC_BYTES: usize = 1024;
const MAX_SCALAR_BYTES: usize = i16::MAX as usize;
const MAX_MEMBERS: usize = 16 * 1024;
const MAX_MEMBER_SUBSCRIPTIONS: usize = 16 * 1024;
const MAX_ASSIGNMENT_TOPICS: usize = 16 * 1024;
const MAX_PARTITIONS_PER_TOPIC: usize = 1024 * 1024;

pub(super) fn validate_group(
    group: &DescribedGroup,
    retained_limit: usize,
) -> Result<usize, ConsumerGroupDescribeResponseFailure> {
    let diagnostic_bytes = group
        .error_message
        .as_deref()
        .map_or(0, |message| message.len().min(MAX_DIAGNOSTIC_BYTES));
    let mut bytes = size_of::<NormalizedConsumerGroupDescribeResponse>()
        .checked_add(group.group_id.len())
        .and_then(|value| value.checked_add(diagnostic_bytes))
        .ok_or(ConsumerGroupDescribeResponseFailure::ResponseTooLarge)?;
    if group.error_code != 0 {
        return limit(bytes, retained_limit);
    }
    validate_scalar(group.group_state.len())?;
    validate_scalar(group.assignor_name.len())?;
    if group.members.len() > MAX_MEMBERS {
        return Err(ConsumerGroupDescribeResponseFailure::ResponseTooLarge);
    }
    bytes = bytes
        .checked_add(size_of::<ConsumerGroupDescribeDescription>())
        .and_then(|value| value.checked_add(group.group_state.len()))
        .and_then(|value| value.checked_add(group.assignor_name.len()))
        .and_then(|value| {
            group
                .members
                .len()
                .checked_mul(size_of::<ConsumerGroupDescribeMember>())
                .and_then(|owners| value.checked_add(owners))
        })
        .ok_or(ConsumerGroupDescribeResponseFailure::ResponseTooLarge)?;
    bytes = charge_items::<&Member>(bytes, group.members.len())?;
    limit(bytes, retained_limit)?;
    validate_unique_members(&group.members)?;
    for member in &group.members {
        if member.member_id.is_empty() {
            return Err(ConsumerGroupDescribeResponseFailure::EmptyMemberId);
        }
        bytes = validate_member(member, bytes, retained_limit)?;
    }
    limit(bytes, retained_limit)
}

fn validate_member(
    member: &Member,
    mut bytes: usize,
    retained_limit: usize,
) -> Result<usize, ConsumerGroupDescribeResponseFailure> {
    if member
        .instance_id
        .as_ref()
        .is_some_and(kafka_wire_core::StrBytes::is_empty)
    {
        return Err(ConsumerGroupDescribeResponseFailure::EmptyInstanceId);
    }
    for length in member_scalar_lengths(member) {
        validate_scalar(length)?;
        bytes = bytes
            .checked_add(length)
            .ok_or(ConsumerGroupDescribeResponseFailure::ResponseTooLarge)?;
    }
    if member.subscribed_topic_names.len() > MAX_MEMBER_SUBSCRIPTIONS {
        return Err(ConsumerGroupDescribeResponseFailure::ResponseTooLarge);
    }
    bytes = charge_items::<String>(bytes, member.subscribed_topic_names.len())?;
    bytes = charge_items::<&kafka_wire_core::StrBytes>(bytes, member.subscribed_topic_names.len())?;
    limit(bytes, retained_limit)?;
    validate_unique_subscriptions(&member.subscribed_topic_names)?;
    for topic in &member.subscribed_topic_names {
        if topic.is_empty() {
            return Err(ConsumerGroupDescribeResponseFailure::EmptySubscription);
        }
        validate_scalar(topic.len())?;
        bytes = bytes
            .checked_add(topic.len())
            .ok_or(ConsumerGroupDescribeResponseFailure::ResponseTooLarge)?;
    }
    bytes = validate_assignment(&member.assignment, bytes, retained_limit)?;
    validate_assignment(&member.target_assignment, bytes, retained_limit)
}

fn member_scalar_lengths(member: &Member) -> [usize; 6] {
    [
        member.member_id.len(),
        member
            .instance_id
            .as_ref()
            .map_or(0, kafka_wire_core::StrBytes::len),
        member
            .rack_id
            .as_ref()
            .map_or(0, kafka_wire_core::StrBytes::len),
        member.client_id.len(),
        member.client_host.len(),
        member
            .subscribed_topic_regex
            .as_ref()
            .map_or(0, kafka_wire_core::StrBytes::len),
    ]
}

fn validate_assignment(
    assignment: &Assignment,
    mut bytes: usize,
    retained_limit: usize,
) -> Result<usize, ConsumerGroupDescribeResponseFailure> {
    if assignment.topic_partitions.len() > MAX_ASSIGNMENT_TOPICS {
        return Err(ConsumerGroupDescribeResponseFailure::ResponseTooLarge);
    }
    bytes = assignment
        .topic_partitions
        .len()
        .checked_mul(size_of::<ConsumerGroupDescribeTopicPartitions>())
        .and_then(|owners| bytes.checked_add(owners))
        .ok_or(ConsumerGroupDescribeResponseFailure::ResponseTooLarge)?;
    bytes = charge_items::<&TopicPartitions>(bytes, assignment.topic_partitions.len())?;
    limit(bytes, retained_limit)?;
    validate_unique_topics(&assignment.topic_partitions)?;
    for topic in &assignment.topic_partitions {
        validate_topic_identity(topic)?;
        bytes = validate_topic_partitions(topic, bytes, retained_limit)?;
    }
    Ok(bytes)
}

fn validate_topic_identity(
    topic: &TopicPartitions,
) -> Result<(), ConsumerGroupDescribeResponseFailure> {
    if topic.topic_id.is_zero() {
        return Err(ConsumerGroupDescribeResponseFailure::TopicId);
    }
    if topic.topic_name.is_empty() {
        return Err(ConsumerGroupDescribeResponseFailure::EmptyTopicName);
    }
    validate_scalar(topic.topic_name.len())?;
    Ok(())
}

fn validate_topic_partitions(
    topic: &TopicPartitions,
    mut bytes: usize,
    retained_limit: usize,
) -> Result<usize, ConsumerGroupDescribeResponseFailure> {
    if topic.partitions.len() > MAX_PARTITIONS_PER_TOPIC {
        return Err(ConsumerGroupDescribeResponseFailure::ResponseTooLarge);
    }
    bytes = bytes
        .checked_add(topic.topic_name.len())
        .and_then(|value| {
            topic
                .partitions
                .len()
                .checked_mul(size_of::<i32>())
                .and_then(|partitions| value.checked_add(partitions))
                .and_then(|value_with_result| {
                    topic
                        .partitions
                        .len()
                        .checked_mul(size_of::<i32>())
                        .and_then(|scratch| value_with_result.checked_add(scratch))
                })
        })
        .ok_or(ConsumerGroupDescribeResponseFailure::ResponseTooLarge)?;
    limit(bytes, retained_limit)?;
    validate_unique_partitions(&topic.partitions)?;
    Ok(bytes)
}

fn charge_items<T>(
    bytes: usize,
    item_count: usize,
) -> Result<usize, ConsumerGroupDescribeResponseFailure> {
    item_count
        .checked_mul(size_of::<T>())
        .and_then(|items| bytes.checked_add(items))
        .ok_or(ConsumerGroupDescribeResponseFailure::ResponseTooLarge)
}

fn validate_scalar(length: usize) -> Result<(), ConsumerGroupDescribeResponseFailure> {
    (length <= MAX_SCALAR_BYTES)
        .then_some(())
        .ok_or(ConsumerGroupDescribeResponseFailure::ScalarTooLarge)
}

fn limit(
    bytes: usize,
    retained_limit: usize,
) -> Result<usize, ConsumerGroupDescribeResponseFailure> {
    (bytes <= retained_limit)
        .then_some(bytes)
        .ok_or(ConsumerGroupDescribeResponseFailure::ResponseTooLarge)
}
