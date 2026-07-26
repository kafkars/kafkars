//! Bounded conversion of one generated Metadata topic into stable scalar facts.

use core::num::NonZeroI16;

use kafka_client_core::{
    DescribeTopicBrokerError, DescribeTopicOutcome, TopicDescription, TopicPartitionDescription,
};
use kafka_wire::metadata_response::{MetadataResponsePartition, MetadataResponseTopic};

use super::describe_topics_response::DescribeTopicsProtocolFailure;

pub(super) fn normalize_topic(
    requested: &str,
    topic: &MetadataResponseTopic,
) -> Result<DescribeTopicOutcome, DescribeTopicsProtocolFailure> {
    if let Some(code) = NonZeroI16::new(topic.error_code) {
        return Ok(DescribeTopicOutcome::failed(
            canonical_string(requested),
            topic.is_internal,
            DescribeTopicBrokerError::new(code),
        ));
    }
    let mut partitions = topic
        .partitions
        .iter()
        .map(normalize_partition)
        .collect::<Result<Vec<_>, _>>()?;
    partitions.sort_unstable_by_key(TopicPartitionDescription::partition_index);
    if partitions
        .windows(2)
        .any(|pair| pair[0].partition_index() == pair[1].partition_index())
    {
        return Err(DescribeTopicsProtocolFailure::DuplicatePartition);
    }
    let name = canonical_string(requested);
    let topic_id = (!topic.topic_id.is_zero()).then(|| topic.topic_id.to_bytes());
    Ok(DescribeTopicOutcome::described(TopicDescription::new(
        name,
        topic_id,
        topic.is_internal,
        partitions,
    )))
}

fn normalize_partition(
    partition: &MetadataResponsePartition,
) -> Result<TopicPartitionDescription, DescribeTopicsProtocolFailure> {
    if partition.partition_index < 0 {
        return Err(DescribeTopicsProtocolFailure::PartitionIndex);
    }
    let leader_id =
        nullable_nonnegative(partition.leader_id, DescribeTopicsProtocolFailure::LeaderId)?;
    let leader_epoch = nullable_nonnegative(
        partition.leader_epoch,
        DescribeTopicsProtocolFailure::LeaderEpoch,
    )?;
    validate_broker_ids(&partition.replica_nodes)?;
    validate_broker_ids(&partition.isr_nodes)?;
    validate_broker_ids(&partition.offline_replicas)?;
    if leader_id.is_some_and(|leader| !partition.replica_nodes.contains(&leader))
        || partition
            .isr_nodes
            .iter()
            .any(|broker| !partition.replica_nodes.contains(broker))
        || partition
            .offline_replicas
            .iter()
            .any(|broker| !partition.replica_nodes.contains(broker))
    {
        return Err(DescribeTopicsProtocolFailure::ReplicaMembership);
    }
    Ok(TopicPartitionDescription::new(
        partition.partition_index,
        NonZeroI16::new(partition.error_code),
        leader_id,
        leader_epoch,
        partition.replica_nodes.clone(),
        partition.isr_nodes.clone(),
        partition.offline_replicas.clone(),
    ))
}

fn nullable_nonnegative(
    value: i32,
    invalid: DescribeTopicsProtocolFailure,
) -> Result<Option<i32>, DescribeTopicsProtocolFailure> {
    match value {
        -1 => Ok(None),
        0.. => Ok(Some(value)),
        _ => Err(invalid),
    }
}

fn validate_broker_ids(values: &[i32]) -> Result<(), DescribeTopicsProtocolFailure> {
    for (index, value) in values.iter().enumerate() {
        if *value < 0 {
            return Err(DescribeTopicsProtocolFailure::BrokerId);
        }
        if values[..index].contains(value) {
            return Err(DescribeTopicsProtocolFailure::DuplicateBrokerId);
        }
    }
    Ok(())
}

fn canonical_string(value: &str) -> String {
    value.to_owned().into_boxed_str().into_string()
}
