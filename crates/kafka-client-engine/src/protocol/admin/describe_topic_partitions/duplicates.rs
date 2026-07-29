//! Fallible scratch validation for protocol-local API-key 75 duplicates.

use kafka_wire::{
    DescribeTopicPartitionsResponse,
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
};

use super::DescribeTopicPartitionsProtocolFailure;

pub(super) fn validate_response_duplicates(
    response: &DescribeTopicPartitionsResponse,
    retained_limit: usize,
) -> Result<(), DescribeTopicPartitionsProtocolFailure> {
    validate_topic_duplicates(&response.topics, retained_limit)?;
    for topic in &response.topics {
        validate_partition_duplicates(topic, retained_limit)?;
        for partition in &topic.partitions {
            validate_broker_duplicates(partition, retained_limit)?;
        }
    }
    Ok(())
}

fn validate_topic_duplicates(
    topics: &[DescribeTopicPartitionsResponseTopic],
    retained_limit: usize,
) -> Result<(), DescribeTopicPartitionsProtocolFailure> {
    let present = topics.iter().filter(|topic| topic.name.is_some()).count();
    ensure_scratch::<&str>(present, retained_limit)?;
    let mut ordered = Vec::new();
    ordered.try_reserve_exact(present).map_err(|_| {
        DescribeTopicPartitionsProtocolFailure::Allocation {
            field: "topic_names",
            requested: present,
        }
    })?;
    ordered.extend(
        topics
            .iter()
            .filter_map(|topic| topic.name.as_ref().map(|name| name.as_str())),
    );
    ordered.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    if ordered.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(DescribeTopicPartitionsProtocolFailure::DuplicateTopicName);
    }
    Ok(())
}

fn validate_partition_duplicates(
    topic: &DescribeTopicPartitionsResponseTopic,
    retained_limit: usize,
) -> Result<(), DescribeTopicPartitionsProtocolFailure> {
    ensure_scratch::<i32>(topic.partitions.len(), retained_limit)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(topic.partitions.len())
        .map_err(|_| DescribeTopicPartitionsProtocolFailure::Allocation {
            field: "partition_indices",
            requested: topic.partitions.len(),
        })?;
    values.extend(
        topic
            .partitions
            .iter()
            .map(|partition| partition.partition_index),
    );
    duplicate_i32(&mut values).map_or(Ok(()), |actual| {
        Err(DescribeTopicPartitionsProtocolFailure::DuplicatePartition { actual })
    })
}

fn validate_broker_duplicates(
    partition: &DescribeTopicPartitionsResponsePartition,
    retained_limit: usize,
) -> Result<(), DescribeTopicPartitionsProtocolFailure> {
    for (field, brokers) in broker_lists(partition) {
        ensure_scratch::<i32>(brokers.len(), retained_limit)?;
        let mut ordered = Vec::new();
        ordered.try_reserve_exact(brokers.len()).map_err(|_| {
            DescribeTopicPartitionsProtocolFailure::Allocation {
                field,
                requested: brokers.len(),
            }
        })?;
        ordered.extend_from_slice(brokers);
        if let Some(actual) = duplicate_i32(&mut ordered) {
            return Err(DescribeTopicPartitionsProtocolFailure::DuplicateBrokerId {
                field,
                actual,
            });
        }
    }
    Ok(())
}

fn duplicate_i32(values: &mut [i32]) -> Option<i32> {
    values.sort_unstable();
    values
        .windows(2)
        .find(|pair| pair[0] == pair[1])
        .map(|pair| pair[0])
}

fn ensure_scratch<T>(
    count: usize,
    retained_limit: usize,
) -> Result<(), DescribeTopicPartitionsProtocolFailure> {
    let required = count
        .checked_mul(core::mem::size_of::<T>())
        .unwrap_or(usize::MAX);
    super::retention::ensure_limit(required, retained_limit)
}

pub(super) fn broker_lists(
    partition: &DescribeTopicPartitionsResponsePartition,
) -> [(&'static str, &[i32]); 5] {
    [
        ("replica_nodes", &partition.replica_nodes),
        ("isr_nodes", &partition.isr_nodes),
        (
            "eligible_leader_replicas",
            partition.eligible_leader_replicas.as_deref().unwrap_or(&[]),
        ),
        (
            "last_known_elr",
            partition.last_known_elr.as_deref().unwrap_or(&[]),
        ),
        ("offline_replicas", &partition.offline_replicas),
    ]
}
