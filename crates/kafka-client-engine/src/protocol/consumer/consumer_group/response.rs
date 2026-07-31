//! Strict API 68 v0 response normalization without membership recovery policy.

use core::num::NonZeroI16;
use std::sync::Arc;

use kafka_wire::{ConsumerGroupHeartbeatResponse, consumer_group_heartbeat_response::Assignment};

use super::model::{
    CONSUMER_GROUP_HEARTBEAT_MAX_PARTITIONS, CONSUMER_GROUP_HEARTBEAT_MAX_TOPICS,
    CONSUMER_GROUP_HEARTBEAT_MAX_VERSION, CONSUMER_GROUP_HEARTBEAT_MIN_VERSION,
    ConsumerGroupHeartbeatAssignmentTopic, ConsumerGroupHeartbeatBrokerRejection,
    ConsumerGroupHeartbeatOutcome, ConsumerGroupHeartbeatSuccess, MAX_KAFKA_STRING_BYTES,
};

/// Generated response facts that cannot safely enter engine or core policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConsumerGroupHeartbeatResponseFailure {
    UnsupportedApiVersion(i16),
    NegativeThrottleTime(i32),
    InvalidMemberId,
    NegativeHeartbeatInterval(i32),
    TooManyTopics { actual: usize, limit: usize },
    TooManyPartitions { actual: usize, limit: usize },
    ZeroTopicId,
    EmptyTopicPartitions,
    DuplicateTopicId,
    NegativePartition(i32),
    DuplicatePartition(u32),
    Allocation,
}

/// Normalizes one selected API 68 v0 response into generated-type-free facts.
pub(crate) fn normalize_consumer_group_heartbeat_response(
    selected_version: i16,
    response: &ConsumerGroupHeartbeatResponse,
) -> Result<ConsumerGroupHeartbeatOutcome, ConsumerGroupHeartbeatResponseFailure> {
    if !(CONSUMER_GROUP_HEARTBEAT_MIN_VERSION..=CONSUMER_GROUP_HEARTBEAT_MAX_VERSION)
        .contains(&selected_version)
    {
        return Err(ConsumerGroupHeartbeatResponseFailure::UnsupportedApiVersion(selected_version));
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_negative| {
        ConsumerGroupHeartbeatResponseFailure::NegativeThrottleTime(response.throttle_time_ms)
    })?;
    if let Some(error_code) = NonZeroI16::new(response.error_code) {
        return Ok(ConsumerGroupHeartbeatOutcome::Rejected(
            ConsumerGroupHeartbeatBrokerRejection::new(throttle_time_ms, error_code),
        ));
    }
    let member_id = response
        .member_id
        .as_ref()
        .map(|member| retain_member(member.as_str()))
        .transpose()?;
    let heartbeat_interval_ms =
        u32::try_from(response.heartbeat_interval_ms).map_err(|_negative| {
            ConsumerGroupHeartbeatResponseFailure::NegativeHeartbeatInterval(
                response.heartbeat_interval_ms,
            )
        })?;
    let assignment = response
        .assignment
        .as_ref()
        .map(normalize_assignment)
        .transpose()?;
    Ok(ConsumerGroupHeartbeatOutcome::Succeeded(
        ConsumerGroupHeartbeatSuccess::new(
            throttle_time_ms,
            member_id,
            response.member_epoch,
            heartbeat_interval_ms,
            assignment,
        ),
    ))
}

fn retain_member(member_id: &str) -> Result<Arc<str>, ConsumerGroupHeartbeatResponseFailure> {
    if member_id.is_empty() || member_id.len() > MAX_KAFKA_STRING_BYTES {
        return Err(ConsumerGroupHeartbeatResponseFailure::InvalidMemberId);
    }
    Ok(Arc::from(member_id))
}

fn normalize_assignment(
    assignment: &Assignment,
) -> Result<Vec<ConsumerGroupHeartbeatAssignmentTopic>, ConsumerGroupHeartbeatResponseFailure> {
    if assignment.topic_partitions.len() > CONSUMER_GROUP_HEARTBEAT_MAX_TOPICS {
        return Err(ConsumerGroupHeartbeatResponseFailure::TooManyTopics {
            actual: assignment.topic_partitions.len(),
            limit: CONSUMER_GROUP_HEARTBEAT_MAX_TOPICS,
        });
    }
    let total = assignment
        .topic_partitions
        .iter()
        .try_fold(0usize, |total, topic| {
            total.checked_add(topic.partitions.len())
        });
    let Some(total) = total else {
        return Err(ConsumerGroupHeartbeatResponseFailure::TooManyPartitions {
            actual: usize::MAX,
            limit: CONSUMER_GROUP_HEARTBEAT_MAX_PARTITIONS,
        });
    };
    if total > CONSUMER_GROUP_HEARTBEAT_MAX_PARTITIONS {
        return Err(ConsumerGroupHeartbeatResponseFailure::TooManyPartitions {
            actual: total,
            limit: CONSUMER_GROUP_HEARTBEAT_MAX_PARTITIONS,
        });
    }
    let mut normalized = Vec::new();
    normalized
        .try_reserve_exact(assignment.topic_partitions.len())
        .map_err(|_allocation| ConsumerGroupHeartbeatResponseFailure::Allocation)?;
    for source in &assignment.topic_partitions {
        let topic_id = source.topic_id.to_bytes();
        if topic_id == [0; 16] {
            return Err(ConsumerGroupHeartbeatResponseFailure::ZeroTopicId);
        }
        if source.partitions.is_empty() {
            return Err(ConsumerGroupHeartbeatResponseFailure::EmptyTopicPartitions);
        }
        if normalized
            .iter()
            .any(|topic: &ConsumerGroupHeartbeatAssignmentTopic| topic.topic_id() == topic_id)
        {
            return Err(ConsumerGroupHeartbeatResponseFailure::DuplicateTopicId);
        }
        let partitions = normalize_partitions(&source.partitions)?;
        normalized.push(ConsumerGroupHeartbeatAssignmentTopic::new(
            topic_id, partitions,
        ));
    }
    normalized.sort_unstable_by_key(ConsumerGroupHeartbeatAssignmentTopic::topic_id);
    Ok(normalized)
}

fn normalize_partitions(source: &[i32]) -> Result<Vec<u32>, ConsumerGroupHeartbeatResponseFailure> {
    let mut partitions = Vec::new();
    partitions
        .try_reserve_exact(source.len())
        .map_err(|_allocation| ConsumerGroupHeartbeatResponseFailure::Allocation)?;
    for partition in source.iter().copied() {
        let partition = u32::try_from(partition).map_err(|_negative| {
            ConsumerGroupHeartbeatResponseFailure::NegativePartition(partition)
        })?;
        if partitions.contains(&partition) {
            return Err(ConsumerGroupHeartbeatResponseFailure::DuplicatePartition(
                partition,
            ));
        }
        partitions.push(partition);
    }
    partitions.sort_unstable();
    Ok(partitions)
}
