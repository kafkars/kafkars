//! Charged response flattening, duplicate rejection, and caller-order restoration.

use core::{cmp::Ordering, num::NonZeroI16};

use kafka_client_core::{
    AlterShareGroupOffsetsPartitionBrokerError, AlterShareGroupOffsetsPartitionOutcome,
    AlterShareGroupOffsetsPlan,
};
use kafka_wire::AlterShareGroupOffsetsResponse;

use super::{
    response::AlterShareGroupOffsetsProtocolFailure,
    retention::{bounded_diagnostic, partition_identity_cmp},
};

#[derive(Clone, Copy)]
pub(super) struct BorrowedPartition<'a> {
    source_topic: usize,
    topic: &'a str,
    topic_id: [u8; 16],
    partition: i32,
    error_code: i16,
    error_message: Option<&'a str>,
}

pub(super) fn collect_partitions<'a>(
    response: &'a AlterShareGroupOffsetsResponse,
    count: usize,
) -> Result<Vec<BorrowedPartition<'a>>, AlterShareGroupOffsetsProtocolFailure> {
    let mut entries = Vec::new();
    entries.try_reserve_exact(count).map_err(|_| {
        AlterShareGroupOffsetsProtocolFailure::Allocation {
            field: "borrowed response partitions",
            requested: count,
        }
    })?;
    for (source_topic, topic) in response.responses.iter().enumerate() {
        entries.extend(topic.partitions.iter().map(|partition| BorrowedPartition {
            source_topic,
            topic: topic.topic_name.as_str(),
            topic_id: topic.topic_id.to_bytes(),
            partition: partition.partition_index,
            error_code: partition.error_code,
            error_message: partition.error_message.as_deref(),
        }));
    }
    Ok(entries)
}

pub(super) fn returned_order(
    left: &BorrowedPartition<'_>,
    right: &BorrowedPartition<'_>,
) -> Ordering {
    partition_identity_cmp(left.topic, left.partition, right.topic, right.partition)
        .then_with(|| left.source_topic.cmp(&right.source_topic))
}

pub(super) fn reject_response_duplicates(
    entries: &[BorrowedPartition<'_>],
) -> Result<(), AlterShareGroupOffsetsProtocolFailure> {
    for pair in entries.windows(2) {
        let [left, right] = pair else {
            continue;
        };
        if left.topic == right.topic && left.source_topic != right.source_topic {
            return Err(AlterShareGroupOffsetsProtocolFailure::DuplicateTopic);
        }
        if left.topic == right.topic && left.partition == right.partition {
            return Err(AlterShareGroupOffsetsProtocolFailure::DuplicatePartition {
                actual: left.partition,
            });
        }
    }
    Ok(())
}

pub(super) fn correlate(
    plan: &AlterShareGroupOffsetsPlan,
    returned: Vec<BorrowedPartition<'_>>,
) -> Result<Vec<AlterShareGroupOffsetsPartitionOutcome>, AlterShareGroupOffsetsProtocolFailure> {
    if returned.len() < plan.changes().len() {
        return Err(AlterShareGroupOffsetsProtocolFailure::MissingPartition);
    }
    if returned.len() > plan.changes().len() {
        return Err(AlterShareGroupOffsetsProtocolFailure::UnexpectedPartition);
    }
    let mut expected = Vec::new();
    expected
        .try_reserve_exact(plan.changes().len())
        .map_err(|_| AlterShareGroupOffsetsProtocolFailure::Allocation {
            field: "expected partition correlation",
            requested: plan.changes().len(),
        })?;
    expected.extend(plan.changes().iter().enumerate());
    expected.sort_unstable_by(|left, right| {
        partition_identity_cmp(
            left.1.topic(),
            left.1.partition(),
            right.1.topic(),
            right.1.partition(),
        )
        .then_with(|| left.0.cmp(&right.0))
    });

    let mut caller_order = Vec::new();
    caller_order
        .try_reserve_exact(plan.changes().len())
        .map_err(|_| AlterShareGroupOffsetsProtocolFailure::Allocation {
            field: "caller partition mapping",
            requested: plan.changes().len(),
        })?;
    for ((caller_index, target), actual) in expected.into_iter().zip(returned) {
        match partition_identity_cmp(
            actual.topic,
            actual.partition,
            target.topic(),
            target.partition(),
        ) {
            Ordering::Less => {
                return Err(AlterShareGroupOffsetsProtocolFailure::UnexpectedPartition);
            }
            Ordering::Greater => {
                return Err(AlterShareGroupOffsetsProtocolFailure::MissingPartition);
            }
            Ordering::Equal => caller_order.push((caller_index, actual)),
        }
    }
    caller_order.sort_unstable_by_key(|(caller_index, _)| *caller_index);
    materialize(caller_order.into_iter().map(|(_, entry)| entry))
}

fn materialize<'a>(
    entries: impl ExactSizeIterator<Item = BorrowedPartition<'a>>,
) -> Result<Vec<AlterShareGroupOffsetsPartitionOutcome>, AlterShareGroupOffsetsProtocolFailure> {
    let mut outcomes = Vec::new();
    outcomes.try_reserve_exact(entries.len()).map_err(|_| {
        AlterShareGroupOffsetsProtocolFailure::Allocation {
            field: "normalized partition outcomes",
            requested: entries.len(),
        }
    })?;
    for entry in entries {
        let outcome = match NonZeroI16::new(entry.error_code) {
            None => AlterShareGroupOffsetsPartitionOutcome::altered(
                entry.topic.to_owned(),
                entry.topic_id,
                entry.partition,
            ),
            Some(code) => {
                let (message, truncated) = bounded_diagnostic(entry.error_message);
                AlterShareGroupOffsetsPartitionOutcome::failed(
                    entry.topic.to_owned(),
                    entry.topic_id,
                    entry.partition,
                    AlterShareGroupOffsetsPartitionBrokerError::new(code, message, truncated),
                )
            }
        };
        outcomes.push(outcome);
    }
    Ok(outcomes)
}
