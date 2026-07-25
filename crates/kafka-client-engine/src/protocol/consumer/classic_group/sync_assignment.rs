//! Fallibly materialized wire assignment bytes from one bounded core Sync plan.

use kafka_client_core::{GroupAssignmentPartition, TopicId};
use kafka_wire::{
    ConsumerProtocolAssignment, consumer_protocol_assignment::TopicPartition as WireTopicPartition,
    encode_consumer_protocol_assignment,
};
use kafka_wire_core::{ApiVersion, BytesMut};

use super::{ClassicSyncRequestFailure, ClassicSyncTopic, validation::INNER_SCHEMA_VERSION};

pub(super) fn materialize_assignment(
    partitions: &[GroupAssignmentPartition],
    topics: &[ClassicSyncTopic],
) -> Result<kafka_wire_core::Bytes, ClassicSyncRequestFailure> {
    let mut grouped = Vec::<WireTopicPartition>::new();
    grouped
        .try_reserve_exact(partitions.len())
        .map_err(|_error| ClassicSyncRequestFailure::Allocation)?;
    let mut start = 0;
    while start < partitions.len() {
        let topic_id = partitions[start].topic_id();
        let end = partitions[start + 1..]
            .iter()
            .position(|partition| partition.topic_id() != topic_id)
            .map_or(partitions.len(), |offset| start + 1 + offset);
        grouped.push(materialize_topic(
            &partitions[start..end],
            topics,
            topic_id,
        )?);
        start = end;
    }
    let mut assignment = ConsumerProtocolAssignment::default();
    assignment.assigned_partitions = grouped;
    assignment.user_data = None;
    let mut encoded = BytesMut::new();
    encode_consumer_protocol_assignment(
        &mut encoded,
        &assignment,
        ApiVersion::new(INNER_SCHEMA_VERSION),
    )
    .map_err(ClassicSyncRequestFailure::Encode)?;
    Ok(encoded.freeze())
}

fn materialize_topic(
    partitions: &[GroupAssignmentPartition],
    topics: &[ClassicSyncTopic],
    topic_id: TopicId,
) -> Result<WireTopicPartition, ClassicSyncRequestFailure> {
    let mut wire = WireTopicPartition::default();
    wire.topic = topic_for_id(topics, topic_id)?.into();
    wire.partitions
        .try_reserve_exact(partitions.len())
        .map_err(|_error| ClassicSyncRequestFailure::Allocation)?;
    for partition in partitions {
        let value = i32::try_from(partition.partition().get()).map_err(|_error| {
            ClassicSyncRequestFailure::PartitionOutOfRange(partition.partition().get())
        })?;
        wire.partitions.push(value);
    }
    Ok(wire)
}

fn topic_for_id(
    topics: &[ClassicSyncTopic],
    topic_id: TopicId,
) -> Result<&str, ClassicSyncRequestFailure> {
    topics
        .iter()
        .find(|topic| topic.topic_id() == topic_id)
        .map(ClassicSyncTopic::topic)
        .ok_or(ClassicSyncRequestFailure::MissingTopic(topic_id))
}
