//! Linear scalar validation before one charged borrowed sort allocation.

use kafka_wire::offset_fetch_response::{OffsetFetchResponseTopic, OffsetFetchResponseTopics};

use super::response::GroupOffsetsProtocolFailure;

pub(super) fn validate_legacy_topics(
    topics: &[OffsetFetchResponseTopic],
    version: i16,
) -> Result<(), GroupOffsetsProtocolFailure> {
    for topic in topics {
        validate_topic(topic.name.as_str(), topic.partitions.is_empty())?;
        for partition in &topic.partitions {
            validate_partition(
                partition.partition_index,
                partition.committed_offset,
                partition.committed_leader_epoch,
                partition.error_code,
                version,
            )?;
        }
    }
    Ok(())
}

pub(super) fn validate_modern_topics(
    topics: &[OffsetFetchResponseTopics],
    version: i16,
) -> Result<(), GroupOffsetsProtocolFailure> {
    for topic in topics {
        validate_topic(topic.name.as_str(), topic.partitions.is_empty())?;
        for partition in &topic.partitions {
            validate_partition(
                partition.partition_index,
                partition.committed_offset,
                partition.committed_leader_epoch,
                partition.error_code,
                version,
            )?;
        }
    }
    Ok(())
}

fn validate_topic(name: &str, partitions_empty: bool) -> Result<(), GroupOffsetsProtocolFailure> {
    if name.is_empty() {
        return Err(GroupOffsetsProtocolFailure::EmptyTopic);
    }
    if partitions_empty {
        return Err(GroupOffsetsProtocolFailure::EmptyTopicPartitions);
    }
    Ok(())
}

fn validate_partition(
    partition: i32,
    offset: i64,
    leader_epoch: i32,
    error_code: i16,
    version: i16,
) -> Result<(), GroupOffsetsProtocolFailure> {
    if partition < 0 {
        return Err(GroupOffsetsProtocolFailure::NegativePartition { actual: partition });
    }
    if error_code == 0 && offset < -1 {
        return Err(GroupOffsetsProtocolFailure::InvalidCommittedOffset { actual: offset });
    }
    if error_code == 0 && version >= 5 && leader_epoch < -1 {
        return Err(GroupOffsetsProtocolFailure::InvalidLeaderEpoch {
            actual: leader_epoch,
        });
    }
    Ok(())
}
