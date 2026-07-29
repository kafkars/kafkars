//! Allocation-aware hostile-shape validation for exact API-77 v1 responses.

use kafka_wire::share_group_describe_response::{DescribedGroup, Member, TopicPartitions};
use kafka_wire_core::StrBytes;

use super::{
    DescribeShareGroupProtocolFailure,
    retention::{
        MAX_ASSIGNMENT_TOPICS_PER_MEMBER, MAX_MEMBERS, MAX_PARTITIONS_PER_TOPIC,
        MAX_RESPONSE_PARTITIONS, MAX_RESPONSE_TEXT_BYTES, MAX_SCALAR_BYTES,
        MAX_SUBSCRIPTIONS_PER_MEMBER,
    },
};

pub(super) fn validate_success_group(
    group: &DescribedGroup,
    include_authorized_operations: bool,
) -> Result<(), DescribeShareGroupProtocolFailure> {
    if group.error_message.is_some() {
        return Err(DescribeShareGroupProtocolFailure::DiagnosticOnSuccess);
    }
    validate_scalar(group.group_state.len())?;
    if group.group_state.is_empty() {
        return Err(DescribeShareGroupProtocolFailure::EmptyGroupState);
    }
    validate_scalar(group.assignor_name.len())?;
    if group.group_epoch < 0 || group.assignment_epoch < 0 {
        return Err(DescribeShareGroupProtocolFailure::NegativeGroupEpoch);
    }
    if !include_authorized_operations && group.authorized_operations != i32::MIN {
        return Err(DescribeShareGroupProtocolFailure::UnexpectedAuthorizedOperations);
    }
    if group.members.len() > MAX_MEMBERS {
        return Err(DescribeShareGroupProtocolFailure::TooManyMembers);
    }
    validate_unique_members(&group.members)?;
    let mut text_bytes = group
        .group_id
        .len()
        .checked_add(group.group_state.len())
        .and_then(|value| value.checked_add(group.assignor_name.len()))
        .ok_or(DescribeShareGroupProtocolFailure::ResponseTextBytesExceeded)?;
    let mut partition_count = 0usize;
    for member in &group.members {
        validate_member(member, &mut text_bytes, &mut partition_count)?;
    }
    Ok(())
}

fn validate_member(
    member: &Member,
    text_bytes: &mut usize,
    partition_count: &mut usize,
) -> Result<(), DescribeShareGroupProtocolFailure> {
    if member.member_id.is_empty() {
        return Err(DescribeShareGroupProtocolFailure::EmptyMemberId);
    }
    if member
        .rack_id
        .as_ref()
        .is_some_and(kafka_wire_core::StrBytes::is_empty)
    {
        return Err(DescribeShareGroupProtocolFailure::EmptyRackId);
    }
    if member.member_epoch < 0 {
        return Err(DescribeShareGroupProtocolFailure::NegativeMemberEpoch);
    }
    for length in [
        member.member_id.len(),
        member
            .rack_id
            .as_ref()
            .map_or(0, kafka_wire_core::StrBytes::len),
        member.client_id.len(),
        member.client_host.len(),
    ] {
        validate_scalar(length)?;
        add_text(text_bytes, length)?;
    }
    if member.subscribed_topic_names.len() > MAX_SUBSCRIPTIONS_PER_MEMBER {
        return Err(DescribeShareGroupProtocolFailure::TooManySubscriptions);
    }
    validate_unique_subscriptions(&member.subscribed_topic_names)?;
    for topic in &member.subscribed_topic_names {
        if topic.is_empty() {
            return Err(DescribeShareGroupProtocolFailure::EmptySubscription);
        }
        validate_scalar(topic.len())?;
        add_text(text_bytes, topic.len())?;
    }
    validate_assignment(member, text_bytes, partition_count)
}

fn validate_assignment(
    member: &Member,
    text_bytes: &mut usize,
    partition_count: &mut usize,
) -> Result<(), DescribeShareGroupProtocolFailure> {
    let topics = &member.assignment.topic_partitions;
    if topics.len() > MAX_ASSIGNMENT_TOPICS_PER_MEMBER {
        return Err(DescribeShareGroupProtocolFailure::TooManyAssignmentTopics);
    }
    validate_unique_topics(topics)?;
    for topic in topics {
        if topic.topic_id.is_zero() {
            return Err(DescribeShareGroupProtocolFailure::ZeroTopicId);
        }
        if topic.topic_name.is_empty() {
            return Err(DescribeShareGroupProtocolFailure::EmptyTopicName);
        }
        validate_scalar(topic.topic_name.len())?;
        add_text(text_bytes, topic.topic_name.len())?;
        if topic.partitions.len() > MAX_PARTITIONS_PER_TOPIC {
            return Err(DescribeShareGroupProtocolFailure::TooManyPartitions);
        }
        *partition_count = partition_count
            .checked_add(topic.partitions.len())
            .ok_or(DescribeShareGroupProtocolFailure::TooManyPartitions)?;
        if *partition_count > MAX_RESPONSE_PARTITIONS {
            return Err(DescribeShareGroupProtocolFailure::TooManyPartitions);
        }
        validate_unique_partitions(&topic.partitions)?;
    }
    Ok(())
}

fn validate_unique_members(members: &[Member]) -> Result<(), DescribeShareGroupProtocolFailure> {
    let mut ordered = try_scratch(members.len())?;
    ordered.extend(members.iter());
    ordered.sort_unstable_by(|left, right| left.member_id.cmp(&right.member_id));
    if ordered
        .windows(2)
        .any(|pair| pair[0].member_id == pair[1].member_id)
    {
        return Err(DescribeShareGroupProtocolFailure::DuplicateMemberId);
    }
    Ok(())
}

fn validate_unique_subscriptions(
    topics: &[StrBytes],
) -> Result<(), DescribeShareGroupProtocolFailure> {
    let mut ordered = try_scratch(topics.len())?;
    ordered.extend(topics.iter());
    ordered.sort_unstable();
    if ordered.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(DescribeShareGroupProtocolFailure::DuplicateSubscription);
    }
    Ok(())
}

fn validate_unique_topics(
    topics: &[TopicPartitions],
) -> Result<(), DescribeShareGroupProtocolFailure> {
    let mut ordered = try_scratch(topics.len())?;
    ordered.extend(topics.iter());
    ordered.sort_unstable_by_key(|topic| topic.topic_id);
    if ordered
        .windows(2)
        .any(|pair| pair[0].topic_id == pair[1].topic_id)
    {
        return Err(DescribeShareGroupProtocolFailure::DuplicateTopicId);
    }
    ordered.sort_unstable_by(|left, right| left.topic_name.cmp(&right.topic_name));
    if ordered
        .windows(2)
        .any(|pair| pair[0].topic_name == pair[1].topic_name)
    {
        return Err(DescribeShareGroupProtocolFailure::DuplicateTopicName);
    }
    Ok(())
}

fn validate_unique_partitions(partitions: &[i32]) -> Result<(), DescribeShareGroupProtocolFailure> {
    let mut ordered = Vec::new();
    ordered
        .try_reserve_exact(partitions.len())
        .map_err(|_| DescribeShareGroupProtocolFailure::Allocation)?;
    ordered.extend(partitions.iter().copied());
    ordered.sort_unstable();
    if ordered.iter().any(|partition| *partition < 0) {
        return Err(DescribeShareGroupProtocolFailure::NegativePartition);
    }
    if ordered.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(DescribeShareGroupProtocolFailure::DuplicatePartition);
    }
    Ok(())
}

fn try_scratch<T>(count: usize) -> Result<Vec<T>, DescribeShareGroupProtocolFailure> {
    let mut scratch = Vec::new();
    scratch
        .try_reserve_exact(count)
        .map_err(|_| DescribeShareGroupProtocolFailure::Allocation)?;
    Ok(scratch)
}

fn validate_scalar(length: usize) -> Result<(), DescribeShareGroupProtocolFailure> {
    (length <= MAX_SCALAR_BYTES)
        .then_some(())
        .ok_or(DescribeShareGroupProtocolFailure::ScalarTooLarge)
}

fn add_text(
    text_bytes: &mut usize,
    length: usize,
) -> Result<(), DescribeShareGroupProtocolFailure> {
    *text_bytes = text_bytes
        .checked_add(length)
        .ok_or(DescribeShareGroupProtocolFailure::ResponseTextBytesExceeded)?;
    (*text_bytes <= MAX_RESPONSE_TEXT_BYTES)
        .then_some(())
        .ok_or(DescribeShareGroupProtocolFailure::ResponseTextBytesExceeded)
}
