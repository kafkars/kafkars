//! Strict API-91 v0 response validation, correlation, and bounded materialization.

use core::num::NonZeroI16;

use kafka_client_core::{
    AlterShareGroupOffsetsBatch, AlterShareGroupOffsetsBrokerError, AlterShareGroupOffsetsPlan,
};
use kafka_wire::AlterShareGroupOffsetsResponse;

use super::{
    ValidatedAlterShareGroupOffsetsResponse,
    correlation::{collect_partitions, correlate, reject_response_duplicates, returned_order},
    retention::{
        MAX_NORMALIZED_BYTES, MAX_RESPONSE_PARTITIONS, MAX_RESPONSE_TEXT_BYTES,
        MAX_RESPONSE_TOPICS, MAX_TOPIC_NAME_BYTES, batch_required_bytes, bounded_diagnostic,
        broker_error_required_bytes, correlation_scratch_bytes,
    },
};

/// Generated response facts unsafe to bind to the accepted API-91 plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterShareGroupOffsetsProtocolFailure {
    MissingSelectedVersion,
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
    EmptyTopicName,
    TopicNameTooLong {
        actual: usize,
        max: usize,
    },
    EmptyTopicPartitions,
    NegativePartition {
        actual: i32,
    },
    DuplicateTopic,
    DuplicatePartition {
        actual: i32,
    },
    MissingPartition,
    UnexpectedPartition,
    ZeroTopicId,
    DiagnosticOnSuccess,
    PartitionsOnTopLevelError,
    ResponseTextBytesExceeded {
        required: usize,
        max: usize,
    },
    NormalizedBytesExceeded {
        required: usize,
        max: usize,
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

/// Validates every bounded fact and restores the exact caller partition order.
pub(crate) fn normalize_alter_share_group_offsets_response(
    plan: &AlterShareGroupOffsetsPlan,
    selected_version: Option<i16>,
    response: &AlterShareGroupOffsetsResponse,
    retained_limit: usize,
) -> Result<ValidatedAlterShareGroupOffsetsResponse, AlterShareGroupOffsetsProtocolFailure> {
    validate_version(selected_version)?;
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        AlterShareGroupOffsetsProtocolFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    let partition_count = validate_source_shape(response)?;

    if let Some(code) = NonZeroI16::new(response.error_code) {
        if partition_count != 0 || !response.responses.is_empty() {
            return Err(AlterShareGroupOffsetsProtocolFailure::PartitionsOnTopLevelError);
        }
        let required = broker_error_required_bytes(response.error_message.as_deref())
            .ok_or(bytes_overflow())?;
        ensure_normalized_limit(required)?;
        ensure_retained_limit(required, retained_limit)?;
        let (message, truncated) = bounded_diagnostic(response.error_message.as_deref());
        let error =
            AlterShareGroupOffsetsBrokerError::new(throttle_time_ms, code, message, truncated);
        return Ok(ValidatedAlterShareGroupOffsetsResponse::BrokerRejected {
            error,
            retained_bytes: required,
        });
    }
    if response.error_message.is_some() {
        return Err(AlterShareGroupOffsetsProtocolFailure::DiagnosticOnSuccess);
    }

    let required = batch_required_bytes(response.responses.iter().flat_map(|topic| {
        topic.partitions.iter().map(|partition| {
            (
                topic.topic_name.as_str(),
                NonZeroI16::new(partition.error_code)
                    .and_then(|_| partition.error_message.as_deref()),
            )
        })
    }))
    .ok_or(bytes_overflow())?;
    ensure_normalized_limit(required)?;
    let peak = required
        .checked_add(correlation_scratch_bytes(partition_count).ok_or(bytes_overflow())?)
        .ok_or(bytes_overflow())?;
    ensure_retained_limit(peak, retained_limit)?;

    let mut returned = collect_partitions(response, partition_count)?;
    returned.sort_unstable_by(returned_order);
    reject_response_duplicates(&returned)?;
    let outcomes = correlate(plan, returned)?;
    let batch = AlterShareGroupOffsetsBatch::new(throttle_time_ms, outcomes);
    Ok(ValidatedAlterShareGroupOffsetsResponse::Batch {
        batch,
        retained_bytes: required,
    })
}

fn validate_version(
    selected_version: Option<i16>,
) -> Result<(), AlterShareGroupOffsetsProtocolFailure> {
    match selected_version {
        None => Err(AlterShareGroupOffsetsProtocolFailure::MissingSelectedVersion),
        Some(0) => Ok(()),
        Some(actual) => {
            Err(AlterShareGroupOffsetsProtocolFailure::UnsupportedApiVersion { actual })
        }
    }
}

fn validate_source_shape(
    response: &AlterShareGroupOffsetsResponse,
) -> Result<usize, AlterShareGroupOffsetsProtocolFailure> {
    if response.responses.len() > MAX_RESPONSE_TOPICS {
        return Err(AlterShareGroupOffsetsProtocolFailure::TooManyTopics {
            actual: response.responses.len(),
            max: MAX_RESPONSE_TOPICS,
        });
    }
    let mut text_bytes = response
        .error_message
        .as_ref()
        .map_or(0, |message| message.len());
    ensure_text_limit(text_bytes)?;
    let mut partition_count = 0usize;
    for topic in &response.responses {
        let name = topic.topic_name.as_str();
        if name.is_empty() {
            return Err(AlterShareGroupOffsetsProtocolFailure::EmptyTopicName);
        }
        if name.len() > MAX_TOPIC_NAME_BYTES {
            return Err(AlterShareGroupOffsetsProtocolFailure::TopicNameTooLong {
                actual: name.len(),
                max: MAX_TOPIC_NAME_BYTES,
            });
        }
        if topic.topic_id.is_zero() {
            return Err(AlterShareGroupOffsetsProtocolFailure::ZeroTopicId);
        }
        if topic.partitions.is_empty() {
            return Err(AlterShareGroupOffsetsProtocolFailure::EmptyTopicPartitions);
        }
        partition_count = partition_count
            .checked_add(topic.partitions.len())
            .ok_or(bytes_overflow())?;
        if partition_count > MAX_RESPONSE_PARTITIONS {
            return Err(AlterShareGroupOffsetsProtocolFailure::TooManyPartitions {
                actual: partition_count,
                max: MAX_RESPONSE_PARTITIONS,
            });
        }
        text_bytes = text_bytes.checked_add(name.len()).ok_or(bytes_overflow())?;
        for partition in &topic.partitions {
            if partition.partition_index < 0 {
                return Err(AlterShareGroupOffsetsProtocolFailure::NegativePartition {
                    actual: partition.partition_index,
                });
            }
            if partition.error_code == 0 && partition.error_message.is_some() {
                return Err(AlterShareGroupOffsetsProtocolFailure::DiagnosticOnSuccess);
            }
            text_bytes = text_bytes
                .checked_add(
                    partition
                        .error_message
                        .as_ref()
                        .map_or(0, |message| message.len()),
                )
                .ok_or(bytes_overflow())?;
        }
        ensure_text_limit(text_bytes)?;
    }
    Ok(partition_count)
}

fn ensure_text_limit(required: usize) -> Result<(), AlterShareGroupOffsetsProtocolFailure> {
    (required <= MAX_RESPONSE_TEXT_BYTES).then_some(()).ok_or(
        AlterShareGroupOffsetsProtocolFailure::ResponseTextBytesExceeded {
            required,
            max: MAX_RESPONSE_TEXT_BYTES,
        },
    )
}

fn ensure_normalized_limit(required: usize) -> Result<(), AlterShareGroupOffsetsProtocolFailure> {
    (required <= MAX_NORMALIZED_BYTES).then_some(()).ok_or(
        AlterShareGroupOffsetsProtocolFailure::NormalizedBytesExceeded {
            required,
            max: MAX_NORMALIZED_BYTES,
        },
    )
}

fn ensure_retained_limit(
    required: usize,
    limit: usize,
) -> Result<(), AlterShareGroupOffsetsProtocolFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(AlterShareGroupOffsetsProtocolFailure::RetainedBytes { required, limit })
}

const fn bytes_overflow() -> AlterShareGroupOffsetsProtocolFailure {
    AlterShareGroupOffsetsProtocolFailure::NormalizedBytesExceeded {
        required: usize::MAX,
        max: MAX_NORMALIZED_BYTES,
    }
}
