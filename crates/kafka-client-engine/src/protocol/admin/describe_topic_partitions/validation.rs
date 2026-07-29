//! Validate-before-allocation checks for generated API-key 75 responses.

use kafka_client_core::{
    DESCRIBE_TOPIC_PARTITIONS_MAX_BROKER_REFERENCES,
    DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS,
    DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_TOPIC_BYTES, DESCRIBE_TOPIC_PARTITIONS_MAX_TOPICS,
};
use kafka_wire::{
    DescribeTopicPartitionsResponse,
    describe_topic_partitions_response::DescribeTopicPartitionsResponsePartition,
};

use super::{
    DescribeTopicPartitionsProtocolFailure,
    duplicates::{broker_lists, validate_response_duplicates},
};

const MAX_TOPIC_NAME_BYTES: usize = i16::MAX as usize;

pub(super) fn validate_response(
    response: &DescribeTopicPartitionsResponse,
    retained_limit: usize,
) -> Result<(), DescribeTopicPartitionsProtocolFailure> {
    if response.topics.len() > DESCRIBE_TOPIC_PARTITIONS_MAX_TOPICS {
        return Err(DescribeTopicPartitionsProtocolFailure::TooManyTopics {
            actual: response.topics.len(),
            max: DESCRIBE_TOPIC_PARTITIONS_MAX_TOPICS,
        });
    }
    let (partitions, broker_references, topic_bytes) = validate_counts(response)?;
    enforce_bounds(
        response.topics.len(),
        partitions,
        broker_references,
        topic_bytes,
    )?;
    validate_response_duplicates(response, retained_limit)
}

fn validate_counts(
    response: &DescribeTopicPartitionsResponse,
) -> Result<(usize, usize, usize), DescribeTopicPartitionsProtocolFailure> {
    let mut partitions = 0usize;
    let mut broker_references = 0usize;
    let mut topic_bytes = 0usize;
    for topic in &response.topics {
        if let Some(name) = topic.name.as_ref() {
            validate_name(name.as_str(), false)?;
            topic_bytes = topic_bytes.checked_add(name.len()).unwrap_or(usize::MAX);
        }
        partitions = partitions
            .checked_add(topic.partitions.len())
            .unwrap_or(usize::MAX);
        for partition in &topic.partitions {
            validate_partition_scalars(partition)?;
            for (field, brokers) in broker_lists(partition) {
                if let Some(actual) = brokers.iter().find(|broker| **broker < 0) {
                    return Err(DescribeTopicPartitionsProtocolFailure::NegativeBrokerId {
                        field,
                        actual: *actual,
                    });
                }
                broker_references = broker_references
                    .checked_add(brokers.len())
                    .unwrap_or(usize::MAX);
            }
        }
    }
    if let Some(cursor) = response.next_cursor.as_ref() {
        validate_name(cursor.topic_name.as_str(), true)?;
        if cursor.partition_index < 0 {
            return Err(
                DescribeTopicPartitionsProtocolFailure::NegativeCursorPartition {
                    actual: cursor.partition_index,
                },
            );
        }
        topic_bytes = topic_bytes
            .checked_add(cursor.topic_name.len())
            .unwrap_or(usize::MAX);
    }
    Ok((partitions, broker_references, topic_bytes))
}

fn enforce_bounds(
    topics: usize,
    partitions: usize,
    broker_references: usize,
    topic_bytes: usize,
) -> Result<(), DescribeTopicPartitionsProtocolFailure> {
    if topics > DESCRIBE_TOPIC_PARTITIONS_MAX_TOPICS {
        return Err(DescribeTopicPartitionsProtocolFailure::TooManyTopics {
            actual: topics,
            max: DESCRIBE_TOPIC_PARTITIONS_MAX_TOPICS,
        });
    }
    if partitions > DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS as usize {
        return Err(DescribeTopicPartitionsProtocolFailure::TooManyPartitions {
            actual: partitions,
            max: DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS as usize,
        });
    }
    if broker_references > DESCRIBE_TOPIC_PARTITIONS_MAX_BROKER_REFERENCES {
        return Err(
            DescribeTopicPartitionsProtocolFailure::TooManyBrokerReferences {
                actual: broker_references,
                max: DESCRIBE_TOPIC_PARTITIONS_MAX_BROKER_REFERENCES,
            },
        );
    }
    if topic_bytes > DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_TOPIC_BYTES {
        return Err(
            DescribeTopicPartitionsProtocolFailure::ResponseTopicBytesExceeded {
                required: topic_bytes,
                max: DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_TOPIC_BYTES,
            },
        );
    }
    Ok(())
}

fn validate_name(name: &str, cursor: bool) -> Result<(), DescribeTopicPartitionsProtocolFailure> {
    if name.is_empty() {
        return Err(if cursor {
            DescribeTopicPartitionsProtocolFailure::EmptyCursorTopic
        } else {
            DescribeTopicPartitionsProtocolFailure::EmptyTopicName
        });
    }
    if name.len() > MAX_TOPIC_NAME_BYTES {
        return Err(if cursor {
            DescribeTopicPartitionsProtocolFailure::CursorTopicTooLong {
                actual: name.len(),
                max: MAX_TOPIC_NAME_BYTES,
            }
        } else {
            DescribeTopicPartitionsProtocolFailure::TopicNameTooLong {
                actual: name.len(),
                max: MAX_TOPIC_NAME_BYTES,
            }
        });
    }
    Ok(())
}

fn validate_partition_scalars(
    partition: &DescribeTopicPartitionsResponsePartition,
) -> Result<(), DescribeTopicPartitionsProtocolFailure> {
    if partition.partition_index < 0 {
        return Err(DescribeTopicPartitionsProtocolFailure::NegativePartition {
            actual: partition.partition_index,
        });
    }
    if partition.leader_id < -1 {
        return Err(DescribeTopicPartitionsProtocolFailure::InvalidLeaderId {
            actual: partition.leader_id,
        });
    }
    if partition.leader_epoch < -1 {
        return Err(DescribeTopicPartitionsProtocolFailure::InvalidLeaderEpoch {
            actual: partition.leader_epoch,
        });
    }
    Ok(())
}
