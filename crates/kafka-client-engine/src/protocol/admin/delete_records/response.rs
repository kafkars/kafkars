//! Strict one-partition Admin `DeleteRecords` response correlation and normalization.

use core::num::NonZeroI16;

use kafka_client_core::{
    DeleteRecordsBrokerError, DeleteRecordsOutcome, DeleteRecordsTarget, DeletedRecords,
};
use kafka_wire::{
    DeleteRecordsResponse,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
};

use super::NormalizedDeleteRecordsResponse;

/// Structural or scalar response facts unsafe to bind to the current target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteRecordsResponseFailure {
    /// The selected version lies outside the supported range.
    UnsupportedApiVersion {
        /// Exact selected Kafka API version.
        actual: i16,
    },
    /// Kafka supplied a negative throttle duration.
    NegativeThrottleTime {
        /// Exact invalid duration.
        actual: i32,
    },
    /// The requested topic result was absent.
    MissingTopic,
    /// The requested topic result appeared more than once.
    DuplicateTopic,
    /// A result named a different topic.
    UnexpectedTopic,
    /// The requested partition result was absent.
    MissingPartition,
    /// The requested partition result appeared more than once.
    DuplicatePartition,
    /// A response partition used a negative index.
    InvalidPartitionIndex {
        /// Exact invalid partition.
        actual: i32,
    },
    /// A result named a different nonnegative partition.
    UnexpectedPartition {
        /// Exact unexpected partition.
        actual: i32,
    },
    /// A successful result supplied a negative low watermark.
    InvalidLowWatermark {
        /// Exact invalid low watermark.
        actual: i64,
    },
}

/// Correlates one generated response before exposing owned scalar facts.
pub(crate) fn normalize_delete_records_response(
    target: &DeleteRecordsTarget,
    selected_version: i16,
    response: &DeleteRecordsResponse,
) -> Result<NormalizedDeleteRecordsResponse, DeleteRecordsResponseFailure> {
    if !(0..=2).contains(&selected_version) {
        return Err(DeleteRecordsResponseFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        DeleteRecordsResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    let topic_response = matching_topic(target.topic(), &response.topics)?;
    let partition_response = matching_partition(target.partition(), &topic_response.partitions)?;
    let outcome = if let Some(code) = NonZeroI16::new(partition_response.error_code) {
        DeleteRecordsOutcome::failed(
            target.topic().to_owned(),
            target.partition(),
            DeleteRecordsBrokerError::new(code),
        )
    } else {
        if partition_response.low_watermark < 0 {
            return Err(DeleteRecordsResponseFailure::InvalidLowWatermark {
                actual: partition_response.low_watermark,
            });
        }
        DeleteRecordsOutcome::deleted(
            target.topic().to_owned(),
            target.partition(),
            DeletedRecords::new(partition_response.low_watermark),
        )
    };
    Ok(NormalizedDeleteRecordsResponse::new(
        throttle_time_ms,
        outcome,
    ))
}

fn matching_topic<'a>(
    expected: &str,
    topics: &'a [DeleteRecordsTopicResult],
) -> Result<&'a DeleteRecordsTopicResult, DeleteRecordsResponseFailure> {
    let mut matching = None;
    for topic in topics {
        if topic.name.as_str() != expected {
            return Err(DeleteRecordsResponseFailure::UnexpectedTopic);
        }
        if matching.replace(topic).is_some() {
            return Err(DeleteRecordsResponseFailure::DuplicateTopic);
        }
    }
    matching.ok_or(DeleteRecordsResponseFailure::MissingTopic)
}

fn matching_partition(
    expected: i32,
    partitions: &[DeleteRecordsPartitionResult],
) -> Result<&DeleteRecordsPartitionResult, DeleteRecordsResponseFailure> {
    let mut matching = None;
    for partition in partitions {
        if partition.partition_index < 0 {
            return Err(DeleteRecordsResponseFailure::InvalidPartitionIndex {
                actual: partition.partition_index,
            });
        }
        if partition.partition_index != expected {
            return Err(DeleteRecordsResponseFailure::UnexpectedPartition {
                actual: partition.partition_index,
            });
        }
        if matching.replace(partition).is_some() {
            return Err(DeleteRecordsResponseFailure::DuplicatePartition);
        }
    }
    matching.ok_or(DeleteRecordsResponseFailure::MissingPartition)
}
