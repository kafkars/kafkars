//! Strict flexible-v0 API-key 75 response normalization.

use kafka_wire::DescribeTopicPartitionsResponse;

use super::{
    NormalizedDescribeTopicPartitionsResponse,
    materialize::materialize_response,
    retention::{ensure_limit, source_response_charge},
    validation::validate_response,
};

/// Unsupported version, hostile shape, invalid scalar, allocation, or capacity failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeTopicPartitionsProtocolFailure {
    UnsupportedApiVersion {
        actual: i16,
    },
    NegativeThrottleTime {
        actual: i32,
    },
    TooManyTopics {
        actual: usize,
        max: usize,
    },
    TooManyPartitions {
        actual: usize,
        max: usize,
    },
    TooManyBrokerReferences {
        actual: usize,
        max: usize,
    },
    ResponseTopicBytesExceeded {
        required: usize,
        max: usize,
    },
    EmptyTopicName,
    TopicNameTooLong {
        actual: usize,
        max: usize,
    },
    DuplicateTopicName,
    NegativePartition {
        actual: i32,
    },
    DuplicatePartition {
        actual: i32,
    },
    InvalidLeaderId {
        actual: i32,
    },
    InvalidLeaderEpoch {
        actual: i32,
    },
    NegativeBrokerId {
        field: &'static str,
        actual: i32,
    },
    DuplicateBrokerId {
        field: &'static str,
        actual: i32,
    },
    EmptyCursorTopic,
    CursorTopicTooLong {
        actual: usize,
        max: usize,
    },
    NegativeCursorPartition {
        actual: i32,
    },
    RetainedBytes {
        required: usize,
        limit: usize,
    },
    Allocation {
        field: &'static str,
        requested: usize,
    },
}

/// Validates and copies one selected-v0 response without request correlation.
pub(crate) fn normalize_describe_topic_partitions_response(
    selected_version: i16,
    response: &DescribeTopicPartitionsResponse,
    retained_limit: usize,
) -> Result<NormalizedDescribeTopicPartitionsResponse, DescribeTopicPartitionsProtocolFailure> {
    if selected_version != 0 {
        return Err(
            DescribeTopicPartitionsProtocolFailure::UnsupportedApiVersion {
                actual: selected_version,
            },
        );
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        DescribeTopicPartitionsProtocolFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    validate_response(response, retained_limit)?;
    let required = source_response_charge(response).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    materialize_response(throttle_time_ms, response, required, retained_limit)
}
