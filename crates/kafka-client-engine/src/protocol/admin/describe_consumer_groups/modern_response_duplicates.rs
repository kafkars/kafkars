//! Charged bounded-sort duplicate checks for modern group response arrays.

use kafka_wire::consumer_group_describe_response::{Member, TopicPartitions};
use kafka_wire_core::StrBytes;

use super::modern_response::ConsumerGroupDescribeResponseFailure;

pub(super) fn validate_unique_members(
    members: &[Member],
) -> Result<(), ConsumerGroupDescribeResponseFailure> {
    let mut ordered = try_scratch(members.len())?;
    ordered.extend(members.iter());
    ordered.sort_unstable_by(|left, right| left.member_id.cmp(&right.member_id));
    if ordered
        .windows(2)
        .any(|pair| pair[0].member_id == pair[1].member_id)
    {
        return Err(ConsumerGroupDescribeResponseFailure::DuplicateMemberId);
    }
    Ok(())
}

pub(super) fn validate_unique_subscriptions(
    topics: &[StrBytes],
) -> Result<(), ConsumerGroupDescribeResponseFailure> {
    let mut ordered = try_scratch(topics.len())?;
    ordered.extend(topics.iter());
    ordered.sort_unstable();
    if ordered.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ConsumerGroupDescribeResponseFailure::DuplicateSubscription);
    }
    Ok(())
}

pub(super) fn validate_unique_topics(
    topics: &[TopicPartitions],
) -> Result<(), ConsumerGroupDescribeResponseFailure> {
    let mut ordered = try_scratch(topics.len())?;
    ordered.extend(topics.iter());
    ordered.sort_unstable_by_key(|topic| topic.topic_id);
    if ordered
        .windows(2)
        .any(|pair| pair[0].topic_id == pair[1].topic_id)
    {
        return Err(ConsumerGroupDescribeResponseFailure::DuplicateTopicId);
    }
    ordered.sort_unstable_by(|left, right| left.topic_name.cmp(&right.topic_name));
    if ordered
        .windows(2)
        .any(|pair| pair[0].topic_name == pair[1].topic_name)
    {
        return Err(ConsumerGroupDescribeResponseFailure::DuplicateTopicName);
    }
    Ok(())
}

pub(super) fn validate_unique_partitions(
    partitions: &[i32],
) -> Result<(), ConsumerGroupDescribeResponseFailure> {
    let mut ordered = try_scratch(partitions.len())?;
    ordered.extend(partitions.iter().copied());
    ordered.sort_unstable();
    if ordered.iter().any(|partition| *partition < 0) {
        return Err(ConsumerGroupDescribeResponseFailure::Partition);
    }
    if ordered.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ConsumerGroupDescribeResponseFailure::DuplicatePartition);
    }
    Ok(())
}

fn try_scratch<T>(item_count: usize) -> Result<Vec<T>, ConsumerGroupDescribeResponseFailure> {
    let mut scratch = Vec::new();
    scratch
        .try_reserve_exact(item_count)
        .map_err(|_| ConsumerGroupDescribeResponseFailure::ResponseTooLarge)?;
    Ok(scratch)
}
