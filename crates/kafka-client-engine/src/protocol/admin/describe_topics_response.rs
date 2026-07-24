//! Ordered bounded normalization of generated Metadata topic results.

use core::num::NonZeroI16;

use kafka_client_core::{
    DescribeTopicBrokerError, DescribeTopicOutcome, DescribeTopicsInput, DescribeTopicsPlan,
    TopicDescription, TopicPartitionDescription,
};
use kafka_wire::{
    MetadataResponse,
    metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
};

use super::describe_topics_budget::ensure_result_fits;

/// Invalid or over-budget generated response shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DescribeTopicsProtocolFailure {
    RetainedBytes,
    TopicCount,
    MissingTopicName,
    UnexpectedTopic,
    MissingTopic,
    DuplicateTopic,
    PartitionIndex,
    DuplicatePartition,
    LeaderId,
    LeaderEpoch,
    BrokerId,
    DuplicateBrokerId,
    ReplicaMembership,
}

/// Converts one generated Metadata response into a deterministic core fact.
pub(crate) fn normalize_describe_topics_response(
    plan: &DescribeTopicsPlan,
    response: &MetadataResponse,
    retained_bytes: usize,
) -> Result<DescribeTopicsInput, DescribeTopicsProtocolFailure> {
    if let Some(code) = NonZeroI16::new(response.error_code) {
        return Ok(DescribeTopicsInput::BrokerRejected { code });
    }
    validate_topic_shape(plan, response)?;
    if !ensure_result_fits(plan, response, retained_bytes) {
        return Err(DescribeTopicsProtocolFailure::RetainedBytes);
    }
    let mut outcomes = Vec::with_capacity(plan.topics().len());
    for requested in plan.topics() {
        let topic = matching_topic(requested, &response.topics)?;
        outcomes.push(normalize_topic(requested, topic)?);
    }
    Ok(DescribeTopicsInput::BrokerResponded { outcomes })
}

fn normalize_topic(
    requested: &str,
    topic: &MetadataResponseTopic,
) -> Result<DescribeTopicOutcome, DescribeTopicsProtocolFailure> {
    if let Some(code) = NonZeroI16::new(topic.error_code) {
        return Ok(DescribeTopicOutcome::failed(
            canonical_string(requested),
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
    Ok(DescribeTopicOutcome::described(
        name.clone(),
        TopicDescription::new(name, topic_id, topic.is_internal, partitions),
    ))
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

fn validate_topic_shape(
    plan: &DescribeTopicsPlan,
    response: &MetadataResponse,
) -> Result<(), DescribeTopicsProtocolFailure> {
    if plan.topics().len() != response.topics.len() {
        return Err(DescribeTopicsProtocolFailure::TopicCount);
    }
    for topic in &response.topics {
        let Some(name) = &topic.name else {
            return Err(DescribeTopicsProtocolFailure::MissingTopicName);
        };
        if !plan
            .topics()
            .iter()
            .any(|requested| requested == name.as_str())
        {
            return Err(DescribeTopicsProtocolFailure::UnexpectedTopic);
        }
    }
    Ok(())
}

fn matching_topic<'a>(
    requested: &str,
    topics: &'a [MetadataResponseTopic],
) -> Result<&'a MetadataResponseTopic, DescribeTopicsProtocolFailure> {
    let mut matches = topics.iter().filter(|topic| {
        topic
            .name
            .as_ref()
            .is_some_and(|name| name.as_str() == requested)
    });
    let Some(topic) = matches.next() else {
        return Err(DescribeTopicsProtocolFailure::MissingTopic);
    };
    if matches.next().is_some() {
        return Err(DescribeTopicsProtocolFailure::DuplicateTopic);
    }
    Ok(topic)
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
