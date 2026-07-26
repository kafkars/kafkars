//! Fallible catalog and allocation preparation before the core start boundary.

use std::sync::Arc;

use kafka_client_core::{GroupAssignmentPartition, GroupPositionPartitionFact, TopicId};

use crate::protocol::consumer::{
    GroupOffsetFetchPreparation, GroupOffsetFetchTopic, PreparedGroupOffsetFetch,
};

use super::{
    super::session_catalog::GroupSessionCatalog, ClassicGroupPositionPreparationError,
    ClassicGroupPositionPreparationMismatch,
};

#[expect(
    clippy::large_enum_variant,
    reason = "the generated request is already bounded and boxing would add hidden allocation"
)]
pub(super) enum RequiredProtocol {
    NoRequest,
    Prepared(PreparedGroupOffsetFetch),
}

pub(super) fn copy_core_partitions(
    assigned: &[GroupAssignmentPartition],
) -> Result<Vec<GroupAssignmentPartition>, ClassicGroupPositionPreparationError> {
    let mut partitions = Vec::new();
    partitions
        .try_reserve_exact(assigned.len())
        .map_err(|_error| ClassicGroupPositionPreparationError::AssignmentCopyAllocation)?;
    partitions.extend_from_slice(assigned);
    Ok(partitions)
}

pub(super) fn reserve_result_buffer(
    partition_count: usize,
) -> Result<Vec<GroupPositionPartitionFact>, ClassicGroupPositionPreparationError> {
    let mut facts = Vec::new();
    facts
        .try_reserve_exact(partition_count)
        .map_err(|_error| ClassicGroupPositionPreparationError::ResultBufferAllocation)?;
    Ok(facts)
}

pub(super) fn prepare_protocol_topics(
    catalog: &GroupSessionCatalog,
    partitions: &[GroupAssignmentPartition],
) -> Result<Vec<GroupOffsetFetchTopic>, ClassicGroupPositionPreparationError> {
    let topic_count = partitions
        .iter()
        .enumerate()
        .filter(|(index, partition)| {
            *index == 0 || partitions[*index - 1].topic_id() != partition.topic_id()
        })
        .count();
    let mut topics = Vec::new();
    topics
        .try_reserve_exact(topic_count)
        .map_err(|_error| ClassicGroupPositionPreparationError::TopicListAllocation)?;

    let mut start = 0usize;
    while start < partitions.len() {
        let topic_id = partitions[start].topic_id();
        let mut end = start + 1;
        while end < partitions.len() && partitions[end].topic_id() == topic_id {
            end += 1;
        }
        topics.push(prepare_topic(catalog, topic_id, &partitions[start..end])?);
        start = end;
    }
    Ok(topics)
}

fn prepare_topic(
    catalog: &GroupSessionCatalog,
    topic_id: TopicId,
    partitions: &[GroupAssignmentPartition],
) -> Result<GroupOffsetFetchTopic, ClassicGroupPositionPreparationError> {
    let mut indexes = Vec::new();
    indexes
        .try_reserve_exact(partitions.len())
        .map_err(|_error| {
            ClassicGroupPositionPreparationError::TopicPartitionListAllocation(topic_id)
        })?;
    for partition in partitions {
        indexes.push(
            i32::try_from(partition.partition().get()).map_err(|_error| {
                ClassicGroupPositionPreparationError::PartitionOutOfRange(partition.partition())
            })?,
        );
    }
    let name = catalog
        .topic_name(topic_id)
        .map(Arc::clone)
        .map_err(ClassicGroupPositionPreparationError::UnknownTopic)?;
    Ok(GroupOffsetFetchTopic::new(name, indexes))
}

pub(super) fn require_protocol_shape(
    protocol: GroupOffsetFetchPreparation,
    assignment_empty: bool,
) -> Result<RequiredProtocol, ClassicGroupPositionPreparationError> {
    match (assignment_empty, protocol) {
        (true, GroupOffsetFetchPreparation::NoRequest) => Ok(RequiredProtocol::NoRequest),
        (false, GroupOffsetFetchPreparation::Prepared(prepared)) => {
            Ok(RequiredProtocol::Prepared(prepared))
        }
        (true, GroupOffsetFetchPreparation::Prepared(_)) => {
            mismatch(ClassicGroupPositionPreparationMismatch::ProtocolRequestForEmptyAssignment)
        }
        (false, GroupOffsetFetchPreparation::NoRequest) => mismatch(
            ClassicGroupPositionPreparationMismatch::ProtocolNoRequestForAssignedPartitions,
        ),
    }
}

fn mismatch<T>(
    mismatch: ClassicGroupPositionPreparationMismatch,
) -> Result<T, ClassicGroupPositionPreparationError> {
    Err(ClassicGroupPositionPreparationError::Mismatch(mismatch))
}
