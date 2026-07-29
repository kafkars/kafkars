//! Checked retained-capacity accounting for normalized API-key 75 pages.

use core::mem::size_of;

use kafka_wire::DescribeTopicPartitionsResponse;

use super::{
    DescribeTopicPartitionsProtocolFailure, NormalizedDescribeTopicPartition,
    NormalizedDescribeTopicPartitionsResponse, NormalizedDescribeTopicPartitionsTopic,
};

pub(super) fn source_response_charge(response: &DescribeTopicPartitionsResponse) -> Option<usize> {
    let topic_owners = response
        .topics
        .len()
        .checked_mul(size_of::<NormalizedDescribeTopicPartitionsTopic>())?;
    let partition_owners = response.topics.iter().try_fold(0usize, |bytes, topic| {
        bytes.checked_add(
            topic
                .partitions
                .len()
                .checked_mul(size_of::<NormalizedDescribeTopicPartition>())?,
        )
    })?;
    let text = response.topics.iter().try_fold(0usize, |bytes, topic| {
        bytes.checked_add(topic.name.as_ref().map_or(0, |name| name.len()))
    })?;
    let text = text.checked_add(
        response
            .next_cursor
            .as_ref()
            .map_or(0, |cursor| cursor.topic_name.len()),
    )?;
    let broker_bytes = response
        .topics
        .iter()
        .flat_map(|topic| &topic.partitions)
        .try_fold(0usize, |count, partition| {
            count
                .checked_add(partition.replica_nodes.len())?
                .checked_add(partition.isr_nodes.len())?
                .checked_add(
                    partition
                        .eligible_leader_replicas
                        .as_ref()
                        .map_or(0, Vec::len),
                )?
                .checked_add(partition.last_known_elr.as_ref().map_or(0, Vec::len))?
                .checked_add(partition.offline_replicas.len())
        })?
        .checked_mul(size_of::<i32>())?;
    size_of::<NormalizedDescribeTopicPartitionsResponse>()
        .checked_add(topic_owners)?
        .checked_add(partition_owners)?
        .checked_add(text)?
        .checked_add(broker_bytes)
}

pub(super) fn normalized_response_charge(
    response: &NormalizedDescribeTopicPartitionsResponse,
) -> Option<usize> {
    let mut required = size_of::<NormalizedDescribeTopicPartitionsResponse>().checked_add(
        response
            .topic_capacity()
            .checked_mul(size_of::<NormalizedDescribeTopicPartitionsTopic>())?,
    )?;
    for topic in response.topics() {
        required = required
            .checked_add(topic.name().map_or(0, |name| name.capacity()))?
            .checked_add(
                topic
                    .partition_capacity()
                    .checked_mul(size_of::<NormalizedDescribeTopicPartition>())?,
            )?;
        for partition in topic.partitions() {
            for broker_capacity in partition.broker_capacities() {
                required = required.checked_add(
                    broker_capacity
                        .checked_mul(size_of::<i32>())
                        .unwrap_or(usize::MAX),
                )?;
            }
        }
    }
    required.checked_add(
        response
            .next_cursor()
            .map_or(0, |cursor| cursor.topic_name().capacity()),
    )
}

pub(super) fn ensure_limit(
    required: usize,
    limit: usize,
) -> Result<(), DescribeTopicPartitionsProtocolFailure> {
    if required > limit {
        return Err(DescribeTopicPartitionsProtocolFailure::RetainedBytes { required, limit });
    }
    Ok(())
}
