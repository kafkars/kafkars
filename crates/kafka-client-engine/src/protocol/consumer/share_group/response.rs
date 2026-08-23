//! Strict `ShareGroupHeartbeat` v1 response normalization without recovery policy.

use core::num::NonZeroI16;
use std::sync::Arc;

use kafka_wire::{ShareGroupHeartbeatResponse, share_group_heartbeat_response::Assignment};

use super::model::{
    MAX_KAFKA_STRING_BYTES, SHARE_GROUP_HEARTBEAT_MAX_PARTITIONS, SHARE_GROUP_HEARTBEAT_MAX_TOPICS,
    SHARE_GROUP_HEARTBEAT_MAX_VERSION, SHARE_GROUP_HEARTBEAT_MIN_VERSION,
    ShareGroupHeartbeatAssignmentTopic, ShareGroupHeartbeatBrokerRejection,
    ShareGroupHeartbeatOutcome, ShareGroupHeartbeatSuccess,
};

/// Generated response facts that cannot safely enter membership policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareGroupHeartbeatResponseFailure {
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

pub(crate) fn normalize_share_group_heartbeat_response(
    selected_version: i16,
    response: &ShareGroupHeartbeatResponse,
) -> Result<ShareGroupHeartbeatOutcome, ShareGroupHeartbeatResponseFailure> {
    if !(SHARE_GROUP_HEARTBEAT_MIN_VERSION..=SHARE_GROUP_HEARTBEAT_MAX_VERSION)
        .contains(&selected_version)
    {
        return Err(ShareGroupHeartbeatResponseFailure::UnsupportedApiVersion(
            selected_version,
        ));
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_negative| {
        ShareGroupHeartbeatResponseFailure::NegativeThrottleTime(response.throttle_time_ms)
    })?;
    if let Some(error_code) = NonZeroI16::new(response.error_code) {
        return Ok(ShareGroupHeartbeatOutcome::Rejected(
            ShareGroupHeartbeatBrokerRejection::new(throttle_time_ms, error_code),
        ));
    }
    let member_id = response
        .member_id
        .as_ref()
        .map(|member| retain_member(member.as_str()))
        .transpose()?;
    let heartbeat_interval_ms =
        u32::try_from(response.heartbeat_interval_ms).map_err(|_negative| {
            ShareGroupHeartbeatResponseFailure::NegativeHeartbeatInterval(
                response.heartbeat_interval_ms,
            )
        })?;
    let assignment = response
        .assignment
        .as_ref()
        .map(normalize_assignment)
        .transpose()?;
    Ok(ShareGroupHeartbeatOutcome::Succeeded(
        ShareGroupHeartbeatSuccess::new(
            throttle_time_ms,
            member_id,
            response.member_epoch,
            heartbeat_interval_ms,
            assignment,
        ),
    ))
}

fn retain_member(member_id: &str) -> Result<Arc<str>, ShareGroupHeartbeatResponseFailure> {
    if member_id.is_empty() || member_id.len() > MAX_KAFKA_STRING_BYTES {
        return Err(ShareGroupHeartbeatResponseFailure::InvalidMemberId);
    }
    Ok(Arc::from(member_id))
}

fn normalize_assignment(
    assignment: &Assignment,
) -> Result<Vec<ShareGroupHeartbeatAssignmentTopic>, ShareGroupHeartbeatResponseFailure> {
    if assignment.topic_partitions.len() > SHARE_GROUP_HEARTBEAT_MAX_TOPICS {
        return Err(ShareGroupHeartbeatResponseFailure::TooManyTopics {
            actual: assignment.topic_partitions.len(),
            limit: SHARE_GROUP_HEARTBEAT_MAX_TOPICS,
        });
    }
    let total = assignment
        .topic_partitions
        .iter()
        .try_fold(0usize, |total, topic| {
            total.checked_add(topic.partitions.len())
        })
        .ok_or(ShareGroupHeartbeatResponseFailure::TooManyPartitions {
            actual: usize::MAX,
            limit: SHARE_GROUP_HEARTBEAT_MAX_PARTITIONS,
        })?;
    if total > SHARE_GROUP_HEARTBEAT_MAX_PARTITIONS {
        return Err(ShareGroupHeartbeatResponseFailure::TooManyPartitions {
            actual: total,
            limit: SHARE_GROUP_HEARTBEAT_MAX_PARTITIONS,
        });
    }
    let mut normalized = Vec::new();
    normalized
        .try_reserve_exact(assignment.topic_partitions.len())
        .map_err(|_allocation| ShareGroupHeartbeatResponseFailure::Allocation)?;
    for source in &assignment.topic_partitions {
        let topic_id = source.topic_id.to_bytes();
        if topic_id == [0; 16] {
            return Err(ShareGroupHeartbeatResponseFailure::ZeroTopicId);
        }
        if source.partitions.is_empty() {
            return Err(ShareGroupHeartbeatResponseFailure::EmptyTopicPartitions);
        }
        if normalized
            .iter()
            .any(|topic: &ShareGroupHeartbeatAssignmentTopic| topic.topic_id() == topic_id)
        {
            return Err(ShareGroupHeartbeatResponseFailure::DuplicateTopicId);
        }
        normalized.push(ShareGroupHeartbeatAssignmentTopic::new(
            topic_id,
            normalize_partitions(&source.partitions)?,
        ));
    }
    normalized.sort_unstable_by_key(ShareGroupHeartbeatAssignmentTopic::topic_id);
    Ok(normalized)
}

fn normalize_partitions(source: &[i32]) -> Result<Vec<u32>, ShareGroupHeartbeatResponseFailure> {
    let mut partitions = Vec::new();
    partitions
        .try_reserve_exact(source.len())
        .map_err(|_allocation| ShareGroupHeartbeatResponseFailure::Allocation)?;
    for raw in source.iter().copied() {
        let partition = u32::try_from(raw)
            .map_err(|_negative| ShareGroupHeartbeatResponseFailure::NegativePartition(raw))?;
        if partitions.contains(&partition) {
            return Err(ShareGroupHeartbeatResponseFailure::DuplicatePartition(
                partition,
            ));
        }
        partitions.push(partition);
    }
    partitions.sort_unstable();
    Ok(partitions)
}
