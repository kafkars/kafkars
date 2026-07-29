//! Fallible generated-to-stable copying for validated API-key 75 pages.

use kafka_wire::{
    DescribeTopicPartitionsResponse,
    describe_topic_partitions_response::{
        Cursor, DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
};

use super::{
    DescribeTopicPartitionsProtocolFailure, NormalizedDescribeTopicPartition,
    NormalizedDescribeTopicPartitionsCursor, NormalizedDescribeTopicPartitionsResponse,
    NormalizedDescribeTopicPartitionsTopic,
    retention::{ensure_limit, normalized_response_charge},
};

pub(super) fn materialize_response(
    throttle_time_ms: u32,
    response: &DescribeTopicPartitionsResponse,
    source_required: usize,
    retained_limit: usize,
) -> Result<NormalizedDescribeTopicPartitionsResponse, DescribeTopicPartitionsProtocolFailure> {
    let mut topics = Vec::new();
    topics
        .try_reserve_exact(response.topics.len())
        .map_err(|_| DescribeTopicPartitionsProtocolFailure::Allocation {
            field: "topics",
            requested: response.topics.len(),
        })?;
    for topic in &response.topics {
        topics.push(copy_topic(topic)?);
    }
    let next_cursor = response.next_cursor.as_ref().map(copy_cursor).transpose()?;
    let provisional = NormalizedDescribeTopicPartitionsResponse::new(
        throttle_time_ms,
        topics,
        next_cursor,
        source_required,
    );
    let normalized = normalized_response_charge(&provisional).unwrap_or(usize::MAX);
    ensure_limit(normalized, retained_limit)?;
    let (throttle, topics, cursor, _) = provisional.into_parts();
    Ok(NormalizedDescribeTopicPartitionsResponse::new(
        throttle,
        topics,
        cursor,
        source_required.max(normalized),
    ))
}

fn copy_topic(
    source: &DescribeTopicPartitionsResponseTopic,
) -> Result<NormalizedDescribeTopicPartitionsTopic, DescribeTopicPartitionsProtocolFailure> {
    let name = source
        .name
        .as_ref()
        .map(|name| copy_string(name.as_str(), "topic_name"))
        .transpose()?;
    let mut partitions = Vec::new();
    partitions
        .try_reserve_exact(source.partitions.len())
        .map_err(|_| DescribeTopicPartitionsProtocolFailure::Allocation {
            field: "partitions",
            requested: source.partitions.len(),
        })?;
    for partition in &source.partitions {
        partitions.push(copy_partition(partition)?);
    }
    Ok(NormalizedDescribeTopicPartitionsTopic::new(
        source.error_code,
        name,
        source.topic_id.to_bytes(),
        source.is_internal,
        partitions,
        source.topic_authorized_operations,
    ))
}

fn copy_partition(
    source: &DescribeTopicPartitionsResponsePartition,
) -> Result<NormalizedDescribeTopicPartition, DescribeTopicPartitionsProtocolFailure> {
    Ok(NormalizedDescribeTopicPartition::new(
        source.error_code,
        source.partition_index,
        normalize_sentinel(source.leader_id),
        normalize_sentinel(source.leader_epoch),
        copy_brokers(&source.replica_nodes, "replica_nodes")?,
        copy_brokers(&source.isr_nodes, "isr_nodes")?,
        source
            .eligible_leader_replicas
            .as_ref()
            .map(|brokers| copy_brokers(brokers, "eligible_leader_replicas"))
            .transpose()?,
        source
            .last_known_elr
            .as_ref()
            .map(|brokers| copy_brokers(brokers, "last_known_elr"))
            .transpose()?,
        copy_brokers(&source.offline_replicas, "offline_replicas")?,
    ))
}

fn copy_cursor(
    source: &Cursor,
) -> Result<NormalizedDescribeTopicPartitionsCursor, DescribeTopicPartitionsProtocolFailure> {
    Ok(NormalizedDescribeTopicPartitionsCursor::new(
        copy_string(source.topic_name.as_str(), "next_cursor_topic")?,
        source.partition_index,
    ))
}

fn copy_brokers(
    source: &[i32],
    field: &'static str,
) -> Result<Vec<i32>, DescribeTopicPartitionsProtocolFailure> {
    let mut copied = Vec::new();
    copied.try_reserve_exact(source.len()).map_err(|_| {
        DescribeTopicPartitionsProtocolFailure::Allocation {
            field,
            requested: source.len(),
        }
    })?;
    copied.extend_from_slice(source);
    Ok(copied)
}

fn copy_string(
    source: &str,
    field: &'static str,
) -> Result<String, DescribeTopicPartitionsProtocolFailure> {
    let mut copied = String::new();
    copied.try_reserve_exact(source.len()).map_err(|_| {
        DescribeTopicPartitionsProtocolFailure::Allocation {
            field,
            requested: source.len(),
        }
    })?;
    copied.push_str(source);
    Ok(copied)
}

const fn normalize_sentinel(value: i32) -> Option<i32> {
    if value == -1 { None } else { Some(value) }
}
