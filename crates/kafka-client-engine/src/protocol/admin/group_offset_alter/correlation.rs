//! Charged sorting and linear merge correlation for `OffsetCommit` results.

use core::{cmp::Ordering, num::NonZeroI16};

use kafka_wire::offset_commit_response::OffsetCommitResponseTopic;

use super::{
    OffsetCommitPartitionRef, OffsetCommitPartitionResult, OffsetCommitTargetRef,
    response::GroupOffsetAlterProtocolFailure, shape::validate_response_shape,
};

#[derive(Clone, Copy)]
pub(super) struct IndexedOffsetCommitTarget<'a> {
    caller_index: usize,
    target: OffsetCommitTargetRef<'a>,
}

#[derive(Clone, Copy)]
pub(super) struct BorrowedOffsetCommitPartition<'a> {
    source_topic: usize,
    topic: &'a str,
    partition: i32,
    error_code: i16,
}

pub(super) fn correlate_response<'a>(
    targets: &[OffsetCommitTargetRef<'_>],
    topics: &'a [OffsetCommitResponseTopic],
    entry_count: usize,
) -> Result<Vec<OffsetCommitPartitionRef<'a>>, GroupOffsetAlterProtocolFailure> {
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
        .map_err(|_| GroupOffsetAlterProtocolFailure::RetainedBytes)?;
    for (expected, returned) in expected.iter().zip(&returned) {
        correlate(expected, returned)?;
        let result = match NonZeroI16::new(returned.error_code) {
            Some(code) => OffsetCommitPartitionResult::Rejected { code },
            None => OffsetCommitPartitionResult::Altered,
        };
        entries.push(OffsetCommitPartitionRef::new(
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
    targets: &[OffsetCommitTargetRef<'a>],
    count: usize,
) -> Result<Vec<IndexedOffsetCommitTarget<'a>>, GroupOffsetAlterProtocolFailure> {
    let mut expected = Vec::new();
    expected
        .try_reserve_exact(count)
        .map_err(|_| GroupOffsetAlterProtocolFailure::RetainedBytes)?;
    expected.extend(
        targets
            .iter()
            .copied()
            .enumerate()
            .map(|(caller_index, target)| IndexedOffsetCommitTarget {
                caller_index,
                target,
            }),
    );
    Ok(expected)
}

fn collect_returned(
    topics: &[OffsetCommitResponseTopic],
    expected_count: usize,
) -> Result<Vec<BorrowedOffsetCommitPartition<'_>>, GroupOffsetAlterProtocolFailure> {
    let actual_count = validate_response_shape(topics, expected_count)?;
    if actual_count != expected_count {
        return Err(GroupOffsetAlterProtocolFailure::PartitionCount {
            expected: expected_count,
            actual: actual_count,
        });
    }
    let mut returned = Vec::new();
    returned
        .try_reserve_exact(actual_count)
        .map_err(|_| GroupOffsetAlterProtocolFailure::RetainedBytes)?;
    for (source_topic, topic) in topics.iter().enumerate() {
        returned.extend(
            topic
                .partitions
                .iter()
                .map(|partition| BorrowedOffsetCommitPartition {
                    source_topic,
                    topic: topic.name.as_str(),
                    partition: partition.partition_index,
                    error_code: partition.error_code,
                }),
        );
    }
    Ok(returned)
}

fn validate_expected(
    expected: &[IndexedOffsetCommitTarget<'_>],
) -> Result<(), GroupOffsetAlterProtocolFailure> {
    for pair in expected.windows(2) {
        let [left, right] = pair else {
            continue;
        };
        if same_target(left.target, right.target) {
            return Err(GroupOffsetAlterProtocolFailure::DuplicateTarget {
                actual: left.target.partition(),
            });
        }
    }
    Ok(())
}

fn validate_returned(
    returned: &[BorrowedOffsetCommitPartition<'_>],
) -> Result<(), GroupOffsetAlterProtocolFailure> {
    for pair in returned.windows(2) {
        let [left, right] = pair else {
            continue;
        };
        if left.topic == right.topic && left.source_topic != right.source_topic {
            return Err(GroupOffsetAlterProtocolFailure::DuplicateTopic);
        }
        if left.topic == right.topic && left.partition == right.partition {
            return Err(GroupOffsetAlterProtocolFailure::DuplicatePartition {
                actual: left.partition,
            });
        }
    }
    Ok(())
}

fn validate_topic_count(
    expected: &[IndexedOffsetCommitTarget<'_>],
    actual: usize,
) -> Result<(), GroupOffsetAlterProtocolFailure> {
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
        Err(GroupOffsetAlterProtocolFailure::TopicCount { expected, actual })
    }
}

fn correlate(
    expected: &IndexedOffsetCommitTarget<'_>,
    returned: &BorrowedOffsetCommitPartition<'_>,
) -> Result<(), GroupOffsetAlterProtocolFailure> {
    match returned
        .topic
        .as_bytes()
        .cmp(expected.target.topic().as_bytes())
    {
        Ordering::Less => return Err(GroupOffsetAlterProtocolFailure::UnexpectedTopic),
        Ordering::Greater => return Err(GroupOffsetAlterProtocolFailure::MissingTopic),
        Ordering::Equal => {}
    }
    match returned.partition.cmp(&expected.target.partition()) {
        Ordering::Less => Err(GroupOffsetAlterProtocolFailure::UnexpectedPartition {
            actual: returned.partition,
        }),
        Ordering::Greater => Err(GroupOffsetAlterProtocolFailure::MissingPartition {
            actual: expected.target.partition(),
        }),
        Ordering::Equal => Ok(()),
    }
}

fn indexed_target_order(
    left: &IndexedOffsetCommitTarget<'_>,
    right: &IndexedOffsetCommitTarget<'_>,
) -> Ordering {
    left.target
        .topic()
        .as_bytes()
        .cmp(right.target.topic().as_bytes())
        .then_with(|| left.target.partition().cmp(&right.target.partition()))
        .then_with(|| left.caller_index.cmp(&right.caller_index))
}

fn borrowed_response_order(
    left: &BorrowedOffsetCommitPartition<'_>,
    right: &BorrowedOffsetCommitPartition<'_>,
) -> Ordering {
    left.topic
        .as_bytes()
        .cmp(right.topic.as_bytes())
        .then_with(|| left.partition.cmp(&right.partition))
        .then_with(|| left.source_topic.cmp(&right.source_topic))
}

fn same_target(left: OffsetCommitTargetRef<'_>, right: OffsetCommitTargetRef<'_>) -> bool {
    left.topic() == right.topic() && left.partition() == right.partition()
}
