//! Strict one-partition `ListOffsets` response correlation and normalization.

use core::num::NonZeroI16;

use kafka_client_core::{NextFetchOffset, PartitionIndex};
use kafka_wire::{
    ListOffsetsResponse,
    list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
};

use super::{ListOffsetsOutcome, ResolvedPosition};

/// Structural or scalar response facts unsafe to bind to a position fence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListOffsetsResponseFailure {
    /// Kafka supplied a negative throttle duration.
    NegativeThrottleTime {
        /// Exact invalid duration.
        actual: i32,
    },
    /// The requested topic result was absent.
    MissingTopic,
    /// The requested topic result appeared more than once.
    DuplicateTopic,
    /// A result named a topic other than the requested topic.
    UnexpectedTopic,
    /// The requested partition result was absent.
    MissingPartition,
    /// The requested partition result appeared more than once.
    DuplicatePartition,
    /// A response partition used a negative protocol sentinel.
    InvalidPartitionIndex {
        /// Exact invalid partition.
        actual: i32,
    },
    /// A result named a different nonnegative partition.
    UnexpectedPartition {
        /// Exact unexpected partition.
        actual: i32,
    },
    /// The requested partition cannot fit Kafka's signed field.
    RequestedPartitionOutOfRange {
        /// Exact core partition.
        actual: u32,
    },
    /// A successful result supplied Kafka's unknown-offset sentinel.
    InvalidOffset {
        /// Exact invalid offset.
        actual: i64,
    },
    /// A timestamp used a value below Kafka's unknown sentinel.
    InvalidTimestamp {
        /// Exact invalid timestamp.
        actual: i64,
    },
    /// A leader epoch used a value below Kafka's unknown sentinel.
    InvalidLeaderEpoch {
        /// Exact invalid epoch.
        actual: i32,
    },
}

/// Correlates one generated response before exposing scalar facts to an interpreter.
pub(crate) fn normalize_list_offsets_response(
    topic: &str,
    partition: PartitionIndex,
    response: &ListOffsetsResponse,
) -> Result<ListOffsetsOutcome, ListOffsetsResponseFailure> {
    if response.throttle_time_ms < 0 {
        return Err(ListOffsetsResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        });
    }
    let topic_response = matching_topic(topic, &response.topics)?;
    let expected_partition = i32::try_from(partition.get()).map_err(|_| {
        ListOffsetsResponseFailure::RequestedPartitionOutOfRange {
            actual: partition.get(),
        }
    })?;
    let partition_response = matching_partition(expected_partition, &topic_response.partitions)?;
    if let Some(code) = NonZeroI16::new(partition_response.error_code) {
        return Ok(ListOffsetsOutcome::BrokerError { code });
    }
    let Some(next_offset) = NextFetchOffset::try_from_raw(partition_response.offset) else {
        return Err(ListOffsetsResponseFailure::InvalidOffset {
            actual: partition_response.offset,
        });
    };
    let timestamp_ms = optional_timestamp(partition_response.timestamp)?;
    let leader_epoch = optional_leader_epoch(partition_response.leader_epoch)?;
    Ok(ListOffsetsOutcome::Resolved(ResolvedPosition::new(
        next_offset,
        timestamp_ms,
        leader_epoch,
    )))
}

fn matching_topic<'a>(
    expected: &str,
    topics: &'a [ListOffsetsTopicResponse],
) -> Result<&'a ListOffsetsTopicResponse, ListOffsetsResponseFailure> {
    let mut matching = None;
    for topic in topics {
        if topic.name.as_str() != expected {
            return Err(ListOffsetsResponseFailure::UnexpectedTopic);
        }
        if matching.replace(topic).is_some() {
            return Err(ListOffsetsResponseFailure::DuplicateTopic);
        }
    }
    matching.ok_or(ListOffsetsResponseFailure::MissingTopic)
}

fn matching_partition(
    expected: i32,
    partitions: &[ListOffsetsPartitionResponse],
) -> Result<&ListOffsetsPartitionResponse, ListOffsetsResponseFailure> {
    let mut matching = None;
    for partition in partitions {
        if partition.partition_index < 0 {
            return Err(ListOffsetsResponseFailure::InvalidPartitionIndex {
                actual: partition.partition_index,
            });
        }
        if partition.partition_index != expected {
            return Err(ListOffsetsResponseFailure::UnexpectedPartition {
                actual: partition.partition_index,
            });
        }
        if matching.replace(partition).is_some() {
            return Err(ListOffsetsResponseFailure::DuplicatePartition);
        }
    }
    matching.ok_or(ListOffsetsResponseFailure::MissingPartition)
}

fn optional_timestamp(value: i64) -> Result<Option<i64>, ListOffsetsResponseFailure> {
    match value {
        -1 => Ok(None),
        0.. => Ok(Some(value)),
        actual => Err(ListOffsetsResponseFailure::InvalidTimestamp { actual }),
    }
}

fn optional_leader_epoch(value: i32) -> Result<Option<i32>, ListOffsetsResponseFailure> {
    match value {
        -1 => Ok(None),
        0.. => Ok(Some(value)),
        actual => Err(ListOffsetsResponseFailure::InvalidLeaderEpoch { actual }),
    }
}
