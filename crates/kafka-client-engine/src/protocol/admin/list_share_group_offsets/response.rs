//! Strict API-90 v0-v1 validation, selection correlation, and materialization.

use core::num::NonZeroI16;

use kafka_client_core::{
    ListShareGroupOffsetsBatch, ListShareGroupOffsetsBrokerError, ListShareGroupOffsetsPlan,
    ListShareGroupOffsetsSelection,
};
use kafka_wire::{
    DescribeShareGroupOffsetsResponse,
    describe_share_group_offsets_response::DescribeShareGroupOffsetsResponseGroup,
};

use super::{
    ValidatedListShareGroupOffsetsResponse,
    correlation::{
        collect_partitions, correlate_selected, materialize, partition_order,
        reject_response_duplicates,
    },
    retention::{
        MAX_NORMALIZED_BYTES, MAX_RESPONSE_PARTITIONS, MAX_RESPONSE_TEXT_BYTES,
        MAX_RESPONSE_TOPICS, MAX_TOPIC_NAME_BYTES, batch_required_bytes, bounded_diagnostic,
        broker_error_required_bytes, scratch_required_bytes,
    },
};

/// Generated response facts unsafe to bind to the accepted API-90 plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListShareGroupOffsetsProtocolFailure {
    MissingSelectedVersion,
    UnsupportedApiVersion {
        actual: i16,
    },
    NegativeThrottleTime {
        actual: i32,
    },
    GroupCount {
        actual: usize,
    },
    UnexpectedGroup,
    DiagnosticOnSuccess,
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
    DuplicateTopic,
    NegativePartition {
        actual: i32,
    },
    DuplicatePartition {
        actual: i32,
    },
    MissingPartition,
    UnexpectedPartition,
    ZeroTopicId,
    InvalidStartOffset {
        actual: i64,
    },
    InvalidLeaderEpoch {
        actual: i32,
    },
    InvalidV0Lag {
        actual: i64,
    },
    InvalidLag {
        actual: i64,
    },
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

/// Validates every bounded fact and applies the plan's exact ordering contract.
pub(crate) fn normalize_list_share_group_offsets_response(
    plan: &ListShareGroupOffsetsPlan,
    selected_version: Option<i16>,
    response: &DescribeShareGroupOffsetsResponse,
    retained_limit: usize,
) -> Result<ValidatedListShareGroupOffsetsResponse, ListShareGroupOffsetsProtocolFailure> {
    let selected_version = validate_version(selected_version)?;
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        ListShareGroupOffsetsProtocolFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    let group = matching_group(plan.group_id(), &response.groups)?;
    validate_envelope_bounds(group)?;
    if let Some(code) = NonZeroI16::new(group.error_code) {
        let required =
            broker_error_required_bytes(group.error_message.as_deref()).ok_or(bytes_overflow())?;
        ensure_normalized_limit(required)?;
        ensure_retained_limit(required, retained_limit)?;
        let (message, truncated) = bounded_diagnostic(group.error_message.as_deref());
        let error =
            ListShareGroupOffsetsBrokerError::new(throttle_time_ms, code, message, truncated);
        return Ok(ValidatedListShareGroupOffsetsResponse::BrokerRejected {
            error,
            retained_bytes: required,
        });
    }
    if group.error_message.is_some() {
        return Err(ListShareGroupOffsetsProtocolFailure::DiagnosticOnSuccess);
    }

    let partition_count = validate_success_shape(group, selected_version)?;
    let required = batch_required_bytes(group.topics.iter().flat_map(|topic| {
        topic.partitions.iter().map(|partition| {
            (
                topic.topic_name.as_str(),
                NonZeroI16::new(partition.error_code).and(partition.error_message.as_deref()),
            )
        })
    }))
    .ok_or(bytes_overflow())?;
    ensure_normalized_limit(required)?;
    let peak = required
        .checked_add(scratch_required_bytes(partition_count).ok_or(bytes_overflow())?)
        .ok_or(bytes_overflow())?;
    ensure_retained_limit(peak, retained_limit)?;

    let mut returned = collect_partitions(group, selected_version, partition_count)?;
    returned.sort_unstable_by(partition_order);
    reject_response_duplicates(&returned)?;
    let outcomes = match plan.selection() {
        ListShareGroupOffsetsSelection::All => materialize(returned.into_iter())?,
        ListShareGroupOffsetsSelection::Selected(targets) => correlate_selected(targets, returned)?,
    };
    let batch = ListShareGroupOffsetsBatch::new(throttle_time_ms, outcomes);
    Ok(ValidatedListShareGroupOffsetsResponse::Batch {
        batch,
        retained_bytes: required,
    })
}

fn validate_envelope_bounds(
    group: &DescribeShareGroupOffsetsResponseGroup,
) -> Result<(), ListShareGroupOffsetsProtocolFailure> {
    if group.topics.len() > MAX_RESPONSE_TOPICS {
        return Err(ListShareGroupOffsetsProtocolFailure::TooManyTopics {
            actual: group.topics.len(),
            max: MAX_RESPONSE_TOPICS,
        });
    }
    let mut text_bytes = group
        .group_id
        .len()
        .checked_add(
            group
                .error_message
                .as_ref()
                .map_or(0, kafka_wire_core::StrBytes::len),
        )
        .ok_or(bytes_overflow())?;
    if text_bytes > MAX_RESPONSE_TEXT_BYTES {
        return Err(
            ListShareGroupOffsetsProtocolFailure::ResponseTextBytesExceeded {
                required: text_bytes,
                max: MAX_RESPONSE_TEXT_BYTES,
            },
        );
    }
    let mut partition_count = 0usize;
    for topic in &group.topics {
        text_bytes = text_bytes
            .checked_add(topic.topic_name.len())
            .ok_or(bytes_overflow())?;
        partition_count = partition_count
            .checked_add(topic.partitions.len())
            .ok_or(bytes_overflow())?;
        if partition_count > MAX_RESPONSE_PARTITIONS {
            return Err(ListShareGroupOffsetsProtocolFailure::TooManyPartitions {
                actual: partition_count,
                max: MAX_RESPONSE_PARTITIONS,
            });
        }
        for partition in &topic.partitions {
            text_bytes = text_bytes
                .checked_add(
                    partition
                        .error_message
                        .as_ref()
                        .map_or(0, kafka_wire_core::StrBytes::len),
                )
                .ok_or(bytes_overflow())?;
        }
        if text_bytes > MAX_RESPONSE_TEXT_BYTES {
            return Err(
                ListShareGroupOffsetsProtocolFailure::ResponseTextBytesExceeded {
                    required: text_bytes,
                    max: MAX_RESPONSE_TEXT_BYTES,
                },
            );
        }
    }
    Ok(())
}

fn validate_version(
    selected_version: Option<i16>,
) -> Result<i16, ListShareGroupOffsetsProtocolFailure> {
    match selected_version {
        None => Err(ListShareGroupOffsetsProtocolFailure::MissingSelectedVersion),
        Some(version @ 0..=1) => Ok(version),
        Some(actual) => Err(ListShareGroupOffsetsProtocolFailure::UnsupportedApiVersion { actual }),
    }
}

fn matching_group<'a>(
    expected: &str,
    groups: &'a [DescribeShareGroupOffsetsResponseGroup],
) -> Result<&'a DescribeShareGroupOffsetsResponseGroup, ListShareGroupOffsetsProtocolFailure> {
    let [group] = groups else {
        return Err(ListShareGroupOffsetsProtocolFailure::GroupCount {
            actual: groups.len(),
        });
    };
    if group.group_id.as_str() != expected {
        return Err(ListShareGroupOffsetsProtocolFailure::UnexpectedGroup);
    }
    Ok(group)
}

fn validate_success_shape(
    group: &DescribeShareGroupOffsetsResponseGroup,
    selected_version: i16,
) -> Result<usize, ListShareGroupOffsetsProtocolFailure> {
    if group.topics.len() > MAX_RESPONSE_TOPICS {
        return Err(ListShareGroupOffsetsProtocolFailure::TooManyTopics {
            actual: group.topics.len(),
            max: MAX_RESPONSE_TOPICS,
        });
    }
    let mut text_bytes = group.group_id.len();
    let mut partition_count = 0usize;
    for topic in &group.topics {
        if topic.topic_name.is_empty() {
            return Err(ListShareGroupOffsetsProtocolFailure::EmptyTopicName);
        }
        if topic.topic_name.len() > MAX_TOPIC_NAME_BYTES {
            return Err(ListShareGroupOffsetsProtocolFailure::TopicNameTooLong {
                actual: topic.topic_name.len(),
                max: MAX_TOPIC_NAME_BYTES,
            });
        }
        if topic.partitions.is_empty() {
            return Err(ListShareGroupOffsetsProtocolFailure::EmptyTopicPartitions);
        }
        if topic.topic_id.is_zero() {
            return Err(ListShareGroupOffsetsProtocolFailure::ZeroTopicId);
        }
        text_bytes = text_bytes
            .checked_add(topic.topic_name.len())
            .ok_or(bytes_overflow())?;
        for partition in &topic.partitions {
            partition_count = partition_count.checked_add(1).ok_or(bytes_overflow())?;
            if partition_count > MAX_RESPONSE_PARTITIONS {
                return Err(ListShareGroupOffsetsProtocolFailure::TooManyPartitions {
                    actual: partition_count,
                    max: MAX_RESPONSE_PARTITIONS,
                });
            }
            if partition.partition_index < 0 {
                return Err(ListShareGroupOffsetsProtocolFailure::NegativePartition {
                    actual: partition.partition_index,
                });
            }
            validate_lag(partition.lag, selected_version)?;
            if partition.error_code == 0 {
                if partition.error_message.is_some() {
                    return Err(ListShareGroupOffsetsProtocolFailure::DiagnosticOnSuccess);
                }
                if partition.start_offset < -1 {
                    return Err(ListShareGroupOffsetsProtocolFailure::InvalidStartOffset {
                        actual: partition.start_offset,
                    });
                }
                if partition.leader_epoch < -1 {
                    return Err(ListShareGroupOffsetsProtocolFailure::InvalidLeaderEpoch {
                        actual: partition.leader_epoch,
                    });
                }
            }
            text_bytes = text_bytes
                .checked_add(
                    partition
                        .error_message
                        .as_ref()
                        .map_or(0, kafka_wire_core::StrBytes::len),
                )
                .ok_or(bytes_overflow())?;
            if text_bytes > MAX_RESPONSE_TEXT_BYTES {
                return Err(
                    ListShareGroupOffsetsProtocolFailure::ResponseTextBytesExceeded {
                        required: text_bytes,
                        max: MAX_RESPONSE_TEXT_BYTES,
                    },
                );
            }
        }
    }
    Ok(partition_count)
}

fn validate_lag(
    actual: i64,
    selected_version: i16,
) -> Result<(), ListShareGroupOffsetsProtocolFailure> {
    if selected_version == 0 && actual != -1 {
        return Err(ListShareGroupOffsetsProtocolFailure::InvalidV0Lag { actual });
    }
    if selected_version == 1 && actual < -1 {
        return Err(ListShareGroupOffsetsProtocolFailure::InvalidLag { actual });
    }
    Ok(())
}

fn ensure_normalized_limit(required: usize) -> Result<(), ListShareGroupOffsetsProtocolFailure> {
    (required <= MAX_NORMALIZED_BYTES).then_some(()).ok_or(
        ListShareGroupOffsetsProtocolFailure::NormalizedBytesExceeded {
            required,
            max: MAX_NORMALIZED_BYTES,
        },
    )
}

fn ensure_retained_limit(
    required: usize,
    limit: usize,
) -> Result<(), ListShareGroupOffsetsProtocolFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(ListShareGroupOffsetsProtocolFailure::RetainedBytes { required, limit })
}

const fn bytes_overflow() -> ListShareGroupOffsetsProtocolFailure {
    ListShareGroupOffsetsProtocolFailure::NormalizedBytesExceeded {
        required: usize::MAX,
        max: MAX_NORMALIZED_BYTES,
    }
}
