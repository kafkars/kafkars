//! Bounded exact-correlation normalization of generated v2-v9 commit results.

use core::num::NonZeroI16;

use kafka_client_core::{GroupOffsetCommitBrokerError, GroupOffsetCommitPartitionOutcome};
use kafka_wire::{OffsetCommitResponse, offset_commit_response::OffsetCommitResponsePartition};

use super::{PreparedGroupOffsetCommit, validation::MAX_GROUP_OFFSET_COMMIT_ENTRIES};

/// Invalid response shape after bounded result capacity was reserved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetCommitProtocolFailure {
    ThrottleTime,
    TopicCount,
    ResultCount,
    UnexpectedTopic,
    DuplicateTopic,
    MissingTopic,
    UnexpectedPartition,
    DuplicatePartition,
    MissingPartition,
}

/// Restores generated results to exact checkpoint order without diagnostics.
pub(crate) fn normalize_group_offset_commit_response(
    mut prepared: PreparedGroupOffsetCommit,
    response: &OffsetCommitResponse,
) -> Result<(u32, Vec<GroupOffsetCommitPartitionOutcome>), GroupOffsetCommitProtocolFailure> {
    let throttle_time_ms = u32::try_from(response.throttle_time_ms)
        .map_err(|_| GroupOffsetCommitProtocolFailure::ThrottleTime)?;
    if response.topics.len() > MAX_GROUP_OFFSET_COMMIT_ENTRIES {
        return Err(GroupOffsetCommitProtocolFailure::TopicCount);
    }
    let result_count = response
        .topics
        .iter()
        .try_fold(0usize, |count, topic| {
            count.checked_add(topic.partitions.len())
        })
        .ok_or(GroupOffsetCommitProtocolFailure::ResultCount)?;
    if result_count != prepared.entries().len() {
        return Err(GroupOffsetCommitProtocolFailure::ResultCount);
    }
    validate_topics(&prepared, response)?;
    let mut outcomes = prepared.take_outcomes();
    for entry in prepared.entries() {
        let topic = matching_topic(entry.topic().as_ref(), response)?;
        let partition = matching_partition(entry.partition_index(), &topic.partitions)?;
        let outcome = match NonZeroI16::new(partition.error_code) {
            Some(code) => GroupOffsetCommitPartitionOutcome::rejected(
                entry.topic_id(),
                entry.partition(),
                GroupOffsetCommitBrokerError::new(code),
            ),
            None => {
                GroupOffsetCommitPartitionOutcome::committed(entry.topic_id(), entry.partition())
            }
        };
        outcomes.push(outcome);
    }
    Ok((throttle_time_ms, outcomes))
}

/// Recognizes an exactly correlated response that rejected every partition on
/// stale coordinator authority and therefore committed no supplied offset.
pub(crate) fn is_exact_group_offset_commit_coordinator_rejection(
    prepared: &PreparedGroupOffsetCommit,
    response: &OffsetCommitResponse,
) -> bool {
    if response.throttle_time_ms < 0
        || response.topics.len() > MAX_GROUP_OFFSET_COMMIT_ENTRIES
        || response.topics.iter().try_fold(0usize, |count, topic| {
            count.checked_add(topic.partitions.len())
        }) != Some(prepared.entries().len())
        || prepared.entries().is_empty()
        || validate_topics(prepared, response).is_err()
    {
        return false;
    }
    prepared.entries().iter().all(|entry| {
        matching_topic(entry.topic().as_ref(), response)
            .and_then(|topic| matching_partition(entry.partition_index(), &topic.partitions))
            .is_ok_and(|partition| matches!(partition.error_code, 15 | 16))
    })
}

fn validate_topics(
    prepared: &PreparedGroupOffsetCommit,
    response: &OffsetCommitResponse,
) -> Result<(), GroupOffsetCommitProtocolFailure> {
    for (index, topic) in response.topics.iter().enumerate() {
        if !prepared
            .entries()
            .iter()
            .any(|entry| entry.topic().as_ref() == topic.name.as_str())
        {
            return Err(GroupOffsetCommitProtocolFailure::UnexpectedTopic);
        }
        if response.topics[..index]
            .iter()
            .any(|previous| previous.name == topic.name)
        {
            return Err(GroupOffsetCommitProtocolFailure::DuplicateTopic);
        }
        for (partition_index, partition) in topic.partitions.iter().enumerate() {
            if !prepared.entries().iter().any(|entry| {
                entry.topic().as_ref() == topic.name.as_str()
                    && entry.partition_index() == partition.partition_index
            }) {
                return Err(GroupOffsetCommitProtocolFailure::UnexpectedPartition);
            }
            if topic.partitions[..partition_index]
                .iter()
                .any(|previous| previous.partition_index == partition.partition_index)
            {
                return Err(GroupOffsetCommitProtocolFailure::DuplicatePartition);
            }
        }
    }
    Ok(())
}

fn matching_topic<'a>(
    name: &str,
    response: &'a OffsetCommitResponse,
) -> Result<
    &'a kafka_wire::offset_commit_response::OffsetCommitResponseTopic,
    GroupOffsetCommitProtocolFailure,
> {
    response
        .topics
        .iter()
        .find(|topic| topic.name.as_str() == name)
        .ok_or(GroupOffsetCommitProtocolFailure::MissingTopic)
}

fn matching_partition(
    partition_index: i32,
    partitions: &[OffsetCommitResponsePartition],
) -> Result<&OffsetCommitResponsePartition, GroupOffsetCommitProtocolFailure> {
    partitions
        .iter()
        .find(|partition| partition.partition_index == partition_index)
        .ok_or(GroupOffsetCommitProtocolFailure::MissingPartition)
}
