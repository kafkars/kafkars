//! Charged sorting and linear merge correlation for offset-deletion results.

use core::{cmp::Ordering, num::NonZeroI16};

use kafka_wire::offset_delete_response::OffsetDeleteResponseTopic;

use super::{
    OffsetDeletePartitionRef, OffsetDeletePartitionResult, OffsetDeleteTargetRef,
    response::GroupOffsetDeleteProtocolFailure,
};

#[derive(Clone, Copy)]
pub(super) struct IndexedOffsetDeleteTarget<'a> {
    caller_index: usize,
    target: OffsetDeleteTargetRef<'a>,
}

#[derive(Clone, Copy)]
pub(super) struct BorrowedOffsetDeletePartition<'a> {
    source_topic: usize,
    topic: &'a str,
    partition: i32,
    error_code: i16,
}

pub(super) fn correlate_response<'a>(
    targets: &[OffsetDeleteTargetRef<'_>],
    topics: &'a [OffsetDeleteResponseTopic],
    entry_count: usize,
) -> Result<Vec<OffsetDeletePartitionRef<'a>>, GroupOffsetDeleteProtocolFailure> {
    let mut expected = collect_expected(targets, entry_count)?;
    let mut returned = collect_returned(topics, entry_count)?;
    expected.sort_unstable_by(indexed_target_order);
    returned.sort_unstable_by(borrowed_response_order);
    validate_expected(&expected)?;
    validate_returned(&returned)?;
    validate_topic_count(&expected, topics.len())?;

    let mut entries = Vec::new();
    entries
        .try_reserve_exact(entry_count)
        .map_err(|_| GroupOffsetDeleteProtocolFailure::RetainedBytes)?;
    for (expected, returned) in expected.iter().zip(&returned) {
        correlate(expected, returned)?;
        let result = match NonZeroI16::new(returned.error_code) {
            Some(code) => OffsetDeletePartitionResult::Rejected { code },
            None => OffsetDeletePartitionResult::Deleted,
        };
        entries.push(OffsetDeletePartitionRef::new(
            returned.topic,
            returned.partition,
            result,
            expected.caller_index,
        ));
    }
    entries.sort_unstable_by_key(|entry| entry.caller_index());
    Ok(entries)
}

fn collect_expected<'a>(
    targets: &[OffsetDeleteTargetRef<'a>],
    count: usize,
) -> Result<Vec<IndexedOffsetDeleteTarget<'a>>, GroupOffsetDeleteProtocolFailure> {
    let mut expected = Vec::new();
    expected
        .try_reserve_exact(count)
        .map_err(|_| GroupOffsetDeleteProtocolFailure::RetainedBytes)?;
    expected.extend(
        targets
            .iter()
            .copied()
            .enumerate()
            .map(|(caller_index, target)| IndexedOffsetDeleteTarget {
                caller_index,
                target,
            }),
    );
    Ok(expected)
}

fn collect_returned(
    topics: &[OffsetDeleteResponseTopic],
    expected_count: usize,
) -> Result<Vec<BorrowedOffsetDeletePartition<'_>>, GroupOffsetDeleteProtocolFailure> {
    let actual_count = validate_response_scalars(topics)?;
    if actual_count != expected_count {
        return Err(GroupOffsetDeleteProtocolFailure::PartitionCount {
            expected: expected_count,
            actual: actual_count,
        });
    }
    let mut returned = Vec::new();
    returned
        .try_reserve_exact(actual_count)
        .map_err(|_| GroupOffsetDeleteProtocolFailure::RetainedBytes)?;
    for (source_topic, topic) in topics.iter().enumerate() {
        returned.extend(
            topic
                .partitions
                .iter()
                .map(|partition| BorrowedOffsetDeletePartition {
                    source_topic,
                    topic: topic.name.as_str(),
                    partition: partition.partition_index,
                    error_code: partition.error_code,
                }),
        );
    }
    Ok(returned)
}

fn validate_response_scalars(
    topics: &[OffsetDeleteResponseTopic],
) -> Result<usize, GroupOffsetDeleteProtocolFailure> {
    let mut count = 0usize;
    for topic in topics {
        if topic.name.is_empty() {
            return Err(GroupOffsetDeleteProtocolFailure::EmptyTopic);
        }
        if topic.partitions.is_empty() {
            return Err(GroupOffsetDeleteProtocolFailure::EmptyTopicPartitions);
        }
        for partition in &topic.partitions {
            if partition.partition_index < 0 {
                return Err(GroupOffsetDeleteProtocolFailure::NegativePartition {
                    actual: partition.partition_index,
                });
            }
            count = count
                .checked_add(1)
                .ok_or(GroupOffsetDeleteProtocolFailure::RetainedBytes)?;
        }
    }
    Ok(count)
}

fn validate_expected(
    expected: &[IndexedOffsetDeleteTarget<'_>],
) -> Result<(), GroupOffsetDeleteProtocolFailure> {
    for pair in expected.windows(2) {
        let [left, right] = pair else {
            continue;
        };
        if same_target(left.target, right.target) {
            return Err(GroupOffsetDeleteProtocolFailure::DuplicateTarget {
                actual: left.target.partition(),
            });
        }
    }
    Ok(())
}

fn validate_returned(
    returned: &[BorrowedOffsetDeletePartition<'_>],
) -> Result<(), GroupOffsetDeleteProtocolFailure> {
    for pair in returned.windows(2) {
        let [left, right] = pair else {
            continue;
        };
        if left.topic == right.topic && left.source_topic != right.source_topic {
            return Err(GroupOffsetDeleteProtocolFailure::DuplicateTopic);
        }
        if left.topic == right.topic && left.partition == right.partition {
            return Err(GroupOffsetDeleteProtocolFailure::DuplicatePartition {
                actual: left.partition,
            });
        }
    }
    Ok(())
}

fn validate_topic_count(
    expected: &[IndexedOffsetDeleteTarget<'_>],
    actual: usize,
) -> Result<(), GroupOffsetDeleteProtocolFailure> {
    let expected = expected
        .iter()
        .enumerate()
        .filter(|(index, target)| {
            *index == 0 || expected[*index - 1].target.topic() != target.target.topic()
        })
        .count();
    if expected == actual {
        Ok(())
    } else {
        Err(GroupOffsetDeleteProtocolFailure::TopicCount { expected, actual })
    }
}

fn correlate(
    expected: &IndexedOffsetDeleteTarget<'_>,
    returned: &BorrowedOffsetDeletePartition<'_>,
) -> Result<(), GroupOffsetDeleteProtocolFailure> {
    match returned
        .topic
        .as_bytes()
        .cmp(expected.target.topic().as_bytes())
    {
        Ordering::Less => return Err(GroupOffsetDeleteProtocolFailure::UnexpectedTopic),
        Ordering::Greater => return Err(GroupOffsetDeleteProtocolFailure::MissingTopic),
        Ordering::Equal => {}
    }
    match returned.partition.cmp(&expected.target.partition()) {
        Ordering::Less => Err(GroupOffsetDeleteProtocolFailure::UnexpectedPartition {
            actual: returned.partition,
        }),
        Ordering::Greater => Err(GroupOffsetDeleteProtocolFailure::MissingPartition {
            actual: expected.target.partition(),
        }),
        Ordering::Equal => Ok(()),
    }
}

fn indexed_target_order(
    left: &IndexedOffsetDeleteTarget<'_>,
    right: &IndexedOffsetDeleteTarget<'_>,
) -> Ordering {
    left.target
        .topic()
        .as_bytes()
        .cmp(right.target.topic().as_bytes())
        .then_with(|| left.target.partition().cmp(&right.target.partition()))
        .then_with(|| left.caller_index.cmp(&right.caller_index))
}

fn borrowed_response_order(
    left: &BorrowedOffsetDeletePartition<'_>,
    right: &BorrowedOffsetDeletePartition<'_>,
) -> Ordering {
    left.topic
        .as_bytes()
        .cmp(right.topic.as_bytes())
        .then_with(|| left.partition.cmp(&right.partition))
        .then_with(|| left.source_topic.cmp(&right.source_topic))
}

fn same_target(left: OffsetDeleteTargetRef<'_>, right: OffsetDeleteTargetRef<'_>) -> bool {
    left.topic() == right.topic() && left.partition() == right.partition()
}
