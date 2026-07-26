//! One charged borrowed sort allocation and adjacent duplicate validation.

use kafka_wire::offset_fetch_response::{OffsetFetchResponseTopic, OffsetFetchResponseTopics};

use super::{
    model::{BorrowedGroupOffset, value_ref},
    response::GroupOffsetsProtocolFailure,
};

pub(super) fn collect_legacy_entries(
    topics: &[OffsetFetchResponseTopic],
    version: i16,
    count: usize,
) -> Result<Vec<BorrowedGroupOffset<'_>>, GroupOffsetsProtocolFailure> {
    let mut entries = reserved_entries(count)?;
    for (source_topic, topic) in topics.iter().enumerate() {
        for partition in &topic.partitions {
            entries.push(BorrowedGroupOffset::new(
                topic.name.as_str(),
                partition.partition_index,
                value_ref(
                    partition.error_code,
                    partition.committed_offset,
                    partition.committed_leader_epoch,
                    partition
                        .metadata
                        .as_ref()
                        .map(kafka_wire_core::StrBytes::as_str),
                    version,
                ),
                source_topic,
            ));
        }
    }
    Ok(entries)
}

pub(super) fn collect_modern_entries(
    topics: &[OffsetFetchResponseTopics],
    version: i16,
    count: usize,
) -> Result<Vec<BorrowedGroupOffset<'_>>, GroupOffsetsProtocolFailure> {
    let mut entries = reserved_entries(count)?;
    for (source_topic, topic) in topics.iter().enumerate() {
        for partition in &topic.partitions {
            entries.push(BorrowedGroupOffset::new(
                topic.name.as_str(),
                partition.partition_index,
                value_ref(
                    partition.error_code,
                    partition.committed_offset,
                    partition.committed_leader_epoch,
                    partition
                        .metadata
                        .as_ref()
                        .map(kafka_wire_core::StrBytes::as_str),
                    version,
                ),
                source_topic,
            ));
        }
    }
    Ok(entries)
}

fn reserved_entries<'a>(
    count: usize,
) -> Result<Vec<BorrowedGroupOffset<'a>>, GroupOffsetsProtocolFailure> {
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(count)
        .map_err(|_| GroupOffsetsProtocolFailure::RetainedBytes)?;
    Ok(entries)
}

pub(super) fn reject_sorted_duplicates(
    entries: &[BorrowedGroupOffset<'_>],
) -> Result<(), GroupOffsetsProtocolFailure> {
    for pair in entries.windows(2) {
        let [left, right] = pair else {
            continue;
        };
        if left.topic() != right.topic() {
            continue;
        }
        if left.source_topic() != right.source_topic() {
            return Err(GroupOffsetsProtocolFailure::DuplicateTopic);
        }
        if left.partition() == right.partition() {
            return Err(GroupOffsetsProtocolFailure::DuplicatePartition {
                actual: left.partition(),
            });
        }
    }
    Ok(())
}
