//! Charged response flattening, duplicate rejection, selection merge, and materialization.

use core::{cmp::Ordering, num::NonZeroI16};

use kafka_client_core::{
    ListShareGroupOffsetDescription, ListShareGroupOffsetOutcome,
    ListShareGroupOffsetsPartitionBrokerError,
};
use kafka_wire::describe_share_group_offsets_response::DescribeShareGroupOffsetsResponseGroup;

use super::{response::ListShareGroupOffsetsProtocolFailure, retention::bounded_diagnostic};

#[derive(Clone, Copy)]
pub(super) struct BorrowedPartition<'a> {
    source_topic: usize,
    topic: &'a str,
    topic_id: [u8; 16],
    partition: i32,
    start_offset: i64,
    leader_epoch: i32,
    lag: i64,
    error_code: i16,
    error_message: Option<&'a str>,
    selected_version: i16,
}

pub(super) fn collect_partitions(
    group: &DescribeShareGroupOffsetsResponseGroup,
    selected_version: i16,
    count: usize,
) -> Result<Vec<BorrowedPartition<'_>>, ListShareGroupOffsetsProtocolFailure> {
    let mut entries = Vec::new();
    entries.try_reserve_exact(count).map_err(|_| {
        ListShareGroupOffsetsProtocolFailure::Allocation {
            field: "borrowed response partitions",
            requested: count,
        }
    })?;
    for (source_topic, topic) in group.topics.iter().enumerate() {
        entries.extend(topic.partitions.iter().map(|partition| BorrowedPartition {
            source_topic,
            topic: topic.topic_name.as_str(),
            topic_id: topic.topic_id.to_bytes(),
            partition: partition.partition_index,
            start_offset: partition.start_offset,
            leader_epoch: partition.leader_epoch,
            lag: partition.lag,
            error_code: partition.error_code,
            error_message: partition.error_message.as_deref(),
            selected_version,
        }));
    }
    Ok(entries)
}

pub(super) fn reject_response_duplicates(
    entries: &[BorrowedPartition<'_>],
) -> Result<(), ListShareGroupOffsetsProtocolFailure> {
    for pair in entries.windows(2) {
        let [left, right] = pair else {
            continue;
        };
        if left.topic == right.topic && left.source_topic != right.source_topic {
            return Err(ListShareGroupOffsetsProtocolFailure::DuplicateTopic);
        }
        if left.topic == right.topic && left.partition == right.partition {
            return Err(ListShareGroupOffsetsProtocolFailure::DuplicatePartition {
                actual: left.partition,
            });
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(super) struct IndexedTarget<'a> {
    caller_index: usize,
    topic: &'a str,
    partition: i32,
}

pub(super) fn correlate_selected(
    targets: &[kafka_client_core::ListShareGroupOffsetTarget],
    returned: Vec<BorrowedPartition<'_>>,
) -> Result<Vec<ListShareGroupOffsetOutcome>, ListShareGroupOffsetsProtocolFailure> {
    if targets.len() != returned.len() {
        return Err(if returned.len() < targets.len() {
            ListShareGroupOffsetsProtocolFailure::MissingPartition
        } else {
            ListShareGroupOffsetsProtocolFailure::UnexpectedPartition
        });
    }
    let mut expected = Vec::new();
    expected.try_reserve_exact(targets.len()).map_err(|_| {
        ListShareGroupOffsetsProtocolFailure::Allocation {
            field: "selected response correlation",
            requested: targets.len(),
        }
    })?;
    expected.extend(
        targets
            .iter()
            .enumerate()
            .map(|(caller_index, target)| IndexedTarget {
                caller_index,
                topic: target.topic(),
                partition: target.partition(),
            }),
    );
    expected.sort_unstable_by(indexed_target_order);
    let mut caller_order = Vec::new();
    caller_order.try_reserve_exact(targets.len()).map_err(|_| {
        ListShareGroupOffsetsProtocolFailure::Allocation {
            field: "selected caller order",
            requested: targets.len(),
        }
    })?;
    for (expected, actual) in expected.into_iter().zip(returned) {
        match partition_identity_cmp(
            actual.topic,
            actual.partition,
            expected.topic,
            expected.partition,
        ) {
            Ordering::Less => {
                return Err(ListShareGroupOffsetsProtocolFailure::UnexpectedPartition);
            }
            Ordering::Greater => {
                return Err(ListShareGroupOffsetsProtocolFailure::MissingPartition);
            }
            Ordering::Equal => caller_order.push((expected.caller_index, actual)),
        }
    }
    caller_order.sort_unstable_by_key(|(caller_index, _)| *caller_index);
    materialize(caller_order.into_iter().map(|(_, entry)| entry))
}

pub(super) fn materialize<'a>(
    entries: impl ExactSizeIterator<Item = BorrowedPartition<'a>>,
) -> Result<Vec<ListShareGroupOffsetOutcome>, ListShareGroupOffsetsProtocolFailure> {
    let mut outcomes = Vec::new();
    outcomes.try_reserve_exact(entries.len()).map_err(|_| {
        ListShareGroupOffsetsProtocolFailure::Allocation {
            field: "normalized partition outcomes",
            requested: entries.len(),
        }
    })?;
    for entry in entries {
        let outcome = match NonZeroI16::new(entry.error_code) {
            None => ListShareGroupOffsetOutcome::described(
                entry.topic.to_owned(),
                entry.topic_id,
                entry.partition,
                ListShareGroupOffsetDescription::new(
                    (entry.start_offset != -1).then_some(entry.start_offset),
                    (entry.leader_epoch != -1).then_some(entry.leader_epoch),
                    (entry.selected_version == 1 && entry.lag != -1).then_some(entry.lag),
                ),
            ),
            Some(code) => {
                let (message, truncated) = bounded_diagnostic(entry.error_message);
                ListShareGroupOffsetOutcome::failed(
                    entry.topic.to_owned(),
                    entry.topic_id,
                    entry.partition,
                    ListShareGroupOffsetsPartitionBrokerError::new(code, message, truncated),
                )
            }
        };
        outcomes.push(outcome);
    }
    Ok(outcomes)
}

pub(super) fn partition_order(
    left: &BorrowedPartition<'_>,
    right: &BorrowedPartition<'_>,
) -> Ordering {
    partition_identity_cmp(left.topic, left.partition, right.topic, right.partition)
        .then_with(|| left.source_topic.cmp(&right.source_topic))
}

fn indexed_target_order(left: &IndexedTarget<'_>, right: &IndexedTarget<'_>) -> Ordering {
    partition_identity_cmp(left.topic, left.partition, right.topic, right.partition)
        .then_with(|| left.caller_index.cmp(&right.caller_index))
}

fn partition_identity_cmp(
    left_topic: &str,
    left_partition: i32,
    right_topic: &str,
    right_partition: i32,
) -> Ordering {
    left_topic
        .as_bytes()
        .cmp(right_topic.as_bytes())
        .then_with(|| left_partition.cmp(&right_partition))
}
