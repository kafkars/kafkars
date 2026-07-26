//! Allocation-free validation of hostile `OffsetCommit` response structure.

use kafka_wire::offset_commit_response::OffsetCommitResponseTopic;

use super::GroupOffsetAlterProtocolFailure;

pub(super) fn validate_response_shape(
    topics: &[OffsetCommitResponseTopic],
    expected_count: usize,
) -> Result<usize, GroupOffsetAlterProtocolFailure> {
    let mut count = 0usize;
    for topic in topics {
        if topic.name.is_empty() {
            return Err(GroupOffsetAlterProtocolFailure::EmptyTopic);
        }
        if topic.partitions.is_empty() {
            return Err(GroupOffsetAlterProtocolFailure::EmptyTopicPartitions);
        }
        for partition in &topic.partitions {
            if partition.partition_index < 0 {
                return Err(GroupOffsetAlterProtocolFailure::NegativePartition {
                    actual: partition.partition_index,
                });
            }
            count = count
                .checked_add(1)
                .ok_or(GroupOffsetAlterProtocolFailure::RetainedBytes)?;
            if count > expected_count {
                return Err(GroupOffsetAlterProtocolFailure::PartitionCount {
                    expected: expected_count,
                    actual: count,
                });
            }
        }
    }
    Ok(count)
}
