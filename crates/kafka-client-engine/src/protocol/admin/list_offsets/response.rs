//! Strict one-partition Admin `ListOffsets` response correlation and normalization.

use core::num::NonZeroI16;

use kafka_client_core::{
    AdminListOffset, AdminListOffsetBrokerError, AdminListOffsetOutcome, AdminListOffsetSpec,
    AdminListOffsetTarget, ReadIsolation,
};
use kafka_wire::{
    ListOffsetsResponse,
    list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
};

use super::{NormalizedAdminListOffsetsResponse, minimum_api_version};

/// Structural or scalar response facts unsafe to bind to the current target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdminListOffsetsResponseFailure {
    /// The driver selected a version outside the supported name-based range.
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
    /// A successful result supplied an offset below Kafka's absence sentinel.
    InvalidOffset {
        /// Exact invalid offset.
        actual: i64,
    },
    /// A timestamp used a value below Kafka's absence sentinel.
    InvalidTimestamp {
        /// Exact invalid timestamp.
        actual: i64,
    },
    /// A leader epoch used a value below Kafka's absence sentinel.
    InvalidLeaderEpoch {
        /// Exact invalid epoch.
        actual: i32,
    },
}

/// Correlates one generated response before exposing owned scalar facts.
pub(crate) fn normalize_admin_list_offsets_response(
    target: &AdminListOffsetTarget,
    read_isolation: ReadIsolation,
    selected_version: i16,
    response: &ListOffsetsResponse,
) -> Result<NormalizedAdminListOffsetsResponse, AdminListOffsetsResponseFailure> {
    if selected_version < minimum_api_version(target, read_isolation) || selected_version > 11 {
        return Err(AdminListOffsetsResponseFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    let throttle_time_ms = if selected_version == 1 {
        0
    } else {
        u32::try_from(response.throttle_time_ms).map_err(|_| {
            AdminListOffsetsResponseFailure::NegativeThrottleTime {
                actual: response.throttle_time_ms,
            }
        })?
    };
    let topic_response = matching_topic(target.topic(), &response.topics)?;
    let partition_response = matching_partition(target.partition(), &topic_response.partitions)?;
    let outcome = if let Some(code) = NonZeroI16::new(partition_response.error_code) {
        AdminListOffsetOutcome::failed(
            target.topic().to_owned(),
            target.partition(),
            AdminListOffsetBrokerError::new(code),
        )
    } else {
        AdminListOffsetOutcome::listed(
            target.topic().to_owned(),
            target.partition(),
            AdminListOffset::new(
                optional_offset(target.spec(), partition_response.offset)?,
                optional_timestamp(partition_response.timestamp)?,
                optional_leader_epoch(selected_version, partition_response.leader_epoch)?,
            ),
        )
    };
    Ok(NormalizedAdminListOffsetsResponse::new(
        throttle_time_ms,
        outcome,
    ))
}

fn matching_topic<'a>(
    expected: &str,
    topics: &'a [ListOffsetsTopicResponse],
) -> Result<&'a ListOffsetsTopicResponse, AdminListOffsetsResponseFailure> {
    let mut matching = None;
    for topic in topics {
        if topic.name.as_str() != expected {
            return Err(AdminListOffsetsResponseFailure::UnexpectedTopic);
        }
        if matching.replace(topic).is_some() {
            return Err(AdminListOffsetsResponseFailure::DuplicateTopic);
        }
    }
    matching.ok_or(AdminListOffsetsResponseFailure::MissingTopic)
}

fn matching_partition(
    expected: i32,
    partitions: &[ListOffsetsPartitionResponse],
) -> Result<&ListOffsetsPartitionResponse, AdminListOffsetsResponseFailure> {
    let mut matching = None;
    for partition in partitions {
        if partition.partition_index < 0 {
            return Err(AdminListOffsetsResponseFailure::InvalidPartitionIndex {
                actual: partition.partition_index,
            });
        }
        if partition.partition_index != expected {
            return Err(AdminListOffsetsResponseFailure::UnexpectedPartition {
                actual: partition.partition_index,
            });
        }
        if matching.replace(partition).is_some() {
            return Err(AdminListOffsetsResponseFailure::DuplicatePartition);
        }
    }
    matching.ok_or(AdminListOffsetsResponseFailure::MissingPartition)
}

fn optional_offset(
    spec: AdminListOffsetSpec,
    value: i64,
) -> Result<Option<i64>, AdminListOffsetsResponseFailure> {
    match value {
        -1 if matches!(
            spec,
            AdminListOffsetSpec::Timestamp(_)
                | AdminListOffsetSpec::MaxTimestamp
                | AdminListOffsetSpec::LatestTiered
                | AdminListOffsetSpec::EarliestPendingUpload
        ) =>
        {
            Ok(None)
        }
        0.. => Ok(Some(value)),
        actual => Err(AdminListOffsetsResponseFailure::InvalidOffset { actual }),
    }
}

fn optional_timestamp(value: i64) -> Result<Option<i64>, AdminListOffsetsResponseFailure> {
    match value {
        -1 => Ok(None),
        0.. => Ok(Some(value)),
        actual => Err(AdminListOffsetsResponseFailure::InvalidTimestamp { actual }),
    }
}

fn optional_leader_epoch(
    selected_version: i16,
    value: i32,
) -> Result<Option<i32>, AdminListOffsetsResponseFailure> {
    if selected_version < 4 {
        return Ok(None);
    }
    match value {
        -1 => Ok(None),
        0.. => Ok(Some(value)),
        actual => Err(AdminListOffsetsResponseFailure::InvalidLeaderEpoch { actual }),
    }
}
