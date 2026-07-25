//! Bounded `SyncGroup` response correlation and assignment normalization.

use core::num::NonZeroI16;
use std::sync::Arc;

use kafka_wire::{SyncGroupResponse, decode_consumer_protocol_assignment};
use kafka_wire_core::DecodeError;

use super::{
    ClassicBrokerRejection, ClassicSyncOutcome, NamedAssignmentPartition,
    validation::{
        INNER_SCHEMA_VERSION, MAX_MEMBER_PARTITIONS, MAX_TOPICS, inner_decode_limits,
        valid_sync_version, valid_topic,
    },
};

/// Generated success facts that cannot safely enter assignment ownership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ClassicSyncResponseFailure {
    UnsupportedApiVersion(i16),
    UnexpectedThrottleTime(i32),
    NegativeThrottleTime(i32),
    UnexpectedProtocolType,
    UnexpectedProtocolName,
    Assignment(DecodeError),
    UnsupportedAssignmentVersion(i16),
    AssignmentUserData,
    TopicCount { actual: usize, limit: usize },
    PartitionCount { actual: usize, limit: usize },
    InvalidTopic,
    DuplicateTopic,
    NegativePartition(i32),
    DuplicatePartition(i32),
    Allocation,
}

/// Normalizes one selected v0-v2 Sync terminal without deciding failure policy.
pub(crate) fn normalize_classic_sync_response(
    selected_version: i16,
    response: &SyncGroupResponse,
) -> Result<ClassicSyncOutcome, ClassicSyncResponseFailure> {
    if !valid_sync_version(selected_version) {
        return Err(ClassicSyncResponseFailure::UnsupportedApiVersion(
            selected_version,
        ));
    }
    let throttle_time_ms = normalize_throttle(selected_version, response.throttle_time_ms)?;
    if let Some(error_code) = NonZeroI16::new(response.error_code) {
        return Ok(ClassicSyncOutcome::Rejected(ClassicBrokerRejection::new(
            throttle_time_ms,
            error_code,
        )));
    }
    if response.protocol_type.is_some() {
        return Err(ClassicSyncResponseFailure::UnexpectedProtocolType);
    }
    if response.protocol_name.is_some() {
        return Err(ClassicSyncResponseFailure::UnexpectedProtocolName);
    }
    let (version, assignment) =
        decode_consumer_protocol_assignment(response.assignment.clone(), inner_decode_limits())
            .map_err(ClassicSyncResponseFailure::Assignment)?;
    if version.value() != INNER_SCHEMA_VERSION {
        return Err(ClassicSyncResponseFailure::UnsupportedAssignmentVersion(
            version.value(),
        ));
    }
    if assignment.user_data.is_some() {
        return Err(ClassicSyncResponseFailure::AssignmentUserData);
    }
    let partitions = normalize_assignment(assignment.assigned_partitions)?;
    Ok(ClassicSyncOutcome::Assigned {
        throttle_time_ms,
        partitions,
    })
}

fn normalize_throttle(
    version: i16,
    throttle_time_ms: i32,
) -> Result<u32, ClassicSyncResponseFailure> {
    if version == 0 && throttle_time_ms != 0 {
        return Err(ClassicSyncResponseFailure::UnexpectedThrottleTime(
            throttle_time_ms,
        ));
    }
    u32::try_from(throttle_time_ms)
        .map_err(|_| ClassicSyncResponseFailure::NegativeThrottleTime(throttle_time_ms))
}

fn normalize_assignment(
    topics: Vec<kafka_wire::consumer_protocol_assignment::TopicPartition>,
) -> Result<Vec<NamedAssignmentPartition>, ClassicSyncResponseFailure> {
    if topics.len() > MAX_TOPICS {
        return Err(ClassicSyncResponseFailure::TopicCount {
            actual: topics.len(),
            limit: MAX_TOPICS,
        });
    }
    let total = topics
        .iter()
        .try_fold(0usize, |sum, topic| sum.checked_add(topic.partitions.len()));
    let Some(total) = total else {
        return Err(ClassicSyncResponseFailure::PartitionCount {
            actual: usize::MAX,
            limit: MAX_MEMBER_PARTITIONS,
        });
    };
    if total > MAX_MEMBER_PARTITIONS {
        return Err(ClassicSyncResponseFailure::PartitionCount {
            actual: total,
            limit: MAX_MEMBER_PARTITIONS,
        });
    }
    validate_assignment(&topics)?;
    let mut normalized = Vec::new();
    normalized
        .try_reserve_exact(total)
        .map_err(|_error| ClassicSyncResponseFailure::Allocation)?;
    for topic in topics {
        let name: Arc<str> = Arc::from(topic.topic.as_str());
        for partition in topic.partitions {
            normalized.push(NamedAssignmentPartition::new(Arc::clone(&name), partition));
        }
    }
    Ok(normalized)
}

fn validate_assignment(
    topics: &[kafka_wire::consumer_protocol_assignment::TopicPartition],
) -> Result<(), ClassicSyncResponseFailure> {
    for (topic_index, topic) in topics.iter().enumerate() {
        if !valid_topic(topic.topic.as_str()) {
            return Err(ClassicSyncResponseFailure::InvalidTopic);
        }
        if topics[..topic_index]
            .iter()
            .any(|previous| previous.topic == topic.topic)
        {
            return Err(ClassicSyncResponseFailure::DuplicateTopic);
        }
        for (partition_index, partition) in topic.partitions.iter().copied().enumerate() {
            if partition < 0 {
                return Err(ClassicSyncResponseFailure::NegativePartition(partition));
            }
            if topic.partitions[..partition_index].contains(&partition) {
                return Err(ClassicSyncResponseFailure::DuplicatePartition(partition));
            }
        }
    }
    Ok(())
}
