//! Fallible bounded materialization of flexible v0 API-key 75 requests.

use core::mem::size_of;

use kafka_client_core::{
    DESCRIBE_TOPIC_PARTITIONS_MAX_REQUEST_TOPIC_BYTES,
    DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS, DESCRIBE_TOPIC_PARTITIONS_MAX_TOPICS,
};
use kafka_wire::{
    DescribeTopicPartitionsRequest, RetainedSize,
    describe_topic_partitions_request::{Cursor, TopicRequest},
};
use kafka_wire_core::StrBytes;

use super::DescribeTopicPartitionsRequestPlan;

const MAX_TOPIC_NAME_BYTES: usize = i16::MAX as usize;

/// Invalid request shape, allocation failure, or insufficient retained capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeTopicPartitionsRequestFailure {
    EmptyTopics,
    TooManyTopics {
        actual: usize,
        max: usize,
    },
    EmptyTopicName,
    TopicNameTooLong {
        actual: usize,
        max: usize,
    },
    TopicBytesExceeded {
        required: usize,
        max: usize,
    },
    DuplicateTopic,
    InvalidResponsePartitionLimit {
        actual: u32,
        max: u32,
    },
    EmptyCursorTopic,
    CursorTopicTooLong {
        actual: usize,
        max: usize,
    },
    NegativeCursorPartition {
        actual: i32,
    },
    CursorTopicNotRequested,
    RetainedBytes {
        required: usize,
        limit: usize,
    },
    Allocation {
        field: &'static str,
        requested: usize,
    },
}

/// Builds one generated flexible-v0 request while preserving caller topic order.
pub(crate) fn describe_topic_partitions_request(
    plan: DescribeTopicPartitionsRequestPlan<'_>,
    retained_limit: usize,
) -> Result<DescribeTopicPartitionsRequest, DescribeTopicPartitionsRequestFailure> {
    validate_request(plan)?;
    let required = request_charge(plan).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;

    let mut topics = Vec::new();
    topics.try_reserve_exact(plan.topics().len()).map_err(|_| {
        DescribeTopicPartitionsRequestFailure::Allocation {
            field: "topics",
            requested: plan.topics().len(),
        }
    })?;
    for topic in plan.topics() {
        let mut generated = TopicRequest::default();
        generated.name = copy_string(topic, "topic_name")?;
        topics.push(generated);
    }
    let cursor = plan
        .cursor()
        .map(|cursor| {
            let mut generated = Cursor::default();
            generated.topic_name = copy_string(cursor.topic_name(), "cursor_topic_name")?;
            generated.partition_index = cursor.partition_index();
            Ok(generated)
        })
        .transpose()?;
    let mut request = DescribeTopicPartitionsRequest::default();
    request.topics = topics;
    request.response_partition_limit =
        i32::try_from(plan.response_partition_limit()).map_err(|_| {
            DescribeTopicPartitionsRequestFailure::InvalidResponsePartitionLimit {
                actual: plan.response_partition_limit(),
                max: DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS,
            }
        })?;
    request.cursor = cursor;
    ensure_limit(request.retained_size().heap_bytes(), retained_limit)?;
    Ok(request)
}

fn validate_request(
    plan: DescribeTopicPartitionsRequestPlan<'_>,
) -> Result<(), DescribeTopicPartitionsRequestFailure> {
    if plan.topics().is_empty() {
        return Err(DescribeTopicPartitionsRequestFailure::EmptyTopics);
    }
    if plan.topics().len() > DESCRIBE_TOPIC_PARTITIONS_MAX_TOPICS {
        return Err(DescribeTopicPartitionsRequestFailure::TooManyTopics {
            actual: plan.topics().len(),
            max: DESCRIBE_TOPIC_PARTITIONS_MAX_TOPICS,
        });
    }
    let mut topic_bytes = 0usize;
    for (index, topic) in plan.topics().iter().enumerate() {
        validate_topic(topic)?;
        topic_bytes = topic_bytes.checked_add(topic.len()).unwrap_or(usize::MAX);
        if topic_bytes > DESCRIBE_TOPIC_PARTITIONS_MAX_REQUEST_TOPIC_BYTES {
            return Err(DescribeTopicPartitionsRequestFailure::TopicBytesExceeded {
                required: topic_bytes,
                max: DESCRIBE_TOPIC_PARTITIONS_MAX_REQUEST_TOPIC_BYTES,
            });
        }
        if plan.topics()[..index].contains(topic) {
            return Err(DescribeTopicPartitionsRequestFailure::DuplicateTopic);
        }
    }
    if !(1..=DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS)
        .contains(&plan.response_partition_limit())
    {
        return Err(
            DescribeTopicPartitionsRequestFailure::InvalidResponsePartitionLimit {
                actual: plan.response_partition_limit(),
                max: DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS,
            },
        );
    }
    validate_cursor(plan)
}

fn validate_cursor(
    plan: DescribeTopicPartitionsRequestPlan<'_>,
) -> Result<(), DescribeTopicPartitionsRequestFailure> {
    let Some(cursor) = plan.cursor() else {
        return Ok(());
    };
    if cursor.topic_name().is_empty() {
        return Err(DescribeTopicPartitionsRequestFailure::EmptyCursorTopic);
    }
    if cursor.topic_name().len() > MAX_TOPIC_NAME_BYTES {
        return Err(DescribeTopicPartitionsRequestFailure::CursorTopicTooLong {
            actual: cursor.topic_name().len(),
            max: MAX_TOPIC_NAME_BYTES,
        });
    }
    if cursor.partition_index() < 0 {
        return Err(
            DescribeTopicPartitionsRequestFailure::NegativeCursorPartition {
                actual: cursor.partition_index(),
            },
        );
    }
    if !plan
        .topics()
        .iter()
        .any(|topic| topic.as_str() == cursor.topic_name())
    {
        return Err(DescribeTopicPartitionsRequestFailure::CursorTopicNotRequested);
    }
    Ok(())
}

fn validate_topic(topic: &str) -> Result<(), DescribeTopicPartitionsRequestFailure> {
    if topic.is_empty() {
        return Err(DescribeTopicPartitionsRequestFailure::EmptyTopicName);
    }
    if topic.len() > MAX_TOPIC_NAME_BYTES {
        return Err(DescribeTopicPartitionsRequestFailure::TopicNameTooLong {
            actual: topic.len(),
            max: MAX_TOPIC_NAME_BYTES,
        });
    }
    Ok(())
}

fn request_charge(plan: DescribeTopicPartitionsRequestPlan<'_>) -> Option<usize> {
    size_of::<DescribeTopicPartitionsRequest>()
        .checked_add(plan.topics().len().checked_mul(size_of::<TopicRequest>())?)?
        .checked_add(
            plan.topics()
                .iter()
                .try_fold(0usize, |bytes, topic| bytes.checked_add(topic.len()))?,
        )?
        .checked_add(plan.cursor().map_or(0, |_| size_of::<Cursor>()))?
        .checked_add(plan.cursor().map_or(0, |cursor| cursor.topic_name().len()))
}

fn copy_string(
    value: &str,
    field: &'static str,
) -> Result<StrBytes, DescribeTopicPartitionsRequestFailure> {
    let mut copied = String::new();
    copied.try_reserve_exact(value.len()).map_err(|_| {
        DescribeTopicPartitionsRequestFailure::Allocation {
            field,
            requested: value.len(),
        }
    })?;
    copied.push_str(value);
    Ok(copied.into())
}

fn ensure_limit(
    required: usize,
    limit: usize,
) -> Result<(), DescribeTopicPartitionsRequestFailure> {
    if required > limit {
        return Err(DescribeTopicPartitionsRequestFailure::RetainedBytes { required, limit });
    }
    Ok(())
}
