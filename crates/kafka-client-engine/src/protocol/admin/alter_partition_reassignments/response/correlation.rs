//! Caller-order correlation for validated partition-reassignment rows.

use core::num::NonZeroI16;

use kafka_client_core::{AlterPartitionReassignmentBrokerError, AlterPartitionReassignmentOutcome};
use kafka_wire::AlterPartitionReassignmentsResponse;
use kafka_wire_core::StrBytes;

use super::{AlterPartitionReassignmentRef, AlterPartitionReassignmentsProtocolFailure};

#[derive(Clone, Copy)]
struct Expected<'a> {
    topic: &'a str,
    partition: i32,
    caller_index: usize,
}

#[derive(Clone, Copy)]
pub(super) struct Returned<'a> {
    topic: &'a str,
    partition: i32,
    error_code: i16,
    error_message: Option<&'a str>,
    source_topic: usize,
}

impl Returned<'_> {
    pub(super) const fn error_message(&self) -> Option<&str> {
        self.error_message
    }
}

pub(super) fn correlate_shape<'response>(
    changes: &[AlterPartitionReassignmentRef<'_>],
    response: &'response AlterPartitionReassignmentsResponse,
) -> Result<Vec<Returned<'response>>, AlterPartitionReassignmentsProtocolFailure> {
    let mut expected = Vec::new();
    expected
        .try_reserve_exact(changes.len())
        .map_err(|_| AlterPartitionReassignmentsProtocolFailure::RetainedBytes)?;
    expected.extend(
        changes
            .iter()
            .copied()
            .enumerate()
            .map(|(caller_index, change)| Expected {
                topic: change.topic(),
                partition: change.partition(),
                caller_index,
            }),
    );
    expected.sort_unstable_by(expected_order);
    if expected
        .windows(2)
        .any(|pair| same_expected(pair[0], pair[1]))
    {
        return Err(AlterPartitionReassignmentsProtocolFailure::DuplicatePartition);
    }
    if unique_expected_topics(&expected) != response.responses.len() {
        return Err(AlterPartitionReassignmentsProtocolFailure::TopicCount);
    }

    let returned_count = response
        .responses
        .iter()
        .try_fold(0usize, |count, topic| {
            count.checked_add(topic.partitions.len())
        })
        .ok_or(AlterPartitionReassignmentsProtocolFailure::PartitionCount)?;
    if returned_count != changes.len() {
        return Err(AlterPartitionReassignmentsProtocolFailure::PartitionCount);
    }
    let mut returned = Vec::new();
    returned
        .try_reserve_exact(returned_count)
        .map_err(|_| AlterPartitionReassignmentsProtocolFailure::RetainedBytes)?;
    for (source_topic, topic) in response.responses.iter().enumerate() {
        returned.extend(topic.partitions.iter().map(|partition| Returned {
            topic: topic.name.as_str(),
            partition: partition.partition_index,
            error_code: partition.error_code,
            error_message: partition.error_message.as_ref().map(StrBytes::as_str),
            source_topic,
        }));
    }
    returned.sort_unstable_by(returned_order);
    validate_returned(&returned)?;

    for (expected, returned) in expected.iter().zip(&returned) {
        match returned.topic.as_bytes().cmp(expected.topic.as_bytes()) {
            core::cmp::Ordering::Less => {
                return Err(AlterPartitionReassignmentsProtocolFailure::UnexpectedTopic);
            }
            core::cmp::Ordering::Greater => {
                return Err(AlterPartitionReassignmentsProtocolFailure::MissingTopic);
            }
            core::cmp::Ordering::Equal => {}
        }
        match returned.partition.cmp(&expected.partition) {
            core::cmp::Ordering::Less => {
                return Err(AlterPartitionReassignmentsProtocolFailure::UnexpectedPartition);
            }
            core::cmp::Ordering::Greater => {
                return Err(AlterPartitionReassignmentsProtocolFailure::MissingPartition);
            }
            core::cmp::Ordering::Equal => {}
        }
    }
    Ok(returned)
}

pub(super) fn normalize_in_caller_order(
    changes: &[AlterPartitionReassignmentRef<'_>],
    returned: &[Returned<'_>],
    bounded_error: fn(NonZeroI16, Option<&str>) -> AlterPartitionReassignmentBrokerError,
) -> Result<Vec<AlterPartitionReassignmentOutcome>, AlterPartitionReassignmentsProtocolFailure> {
    let mut outcomes = Vec::new();
    outcomes
        .try_reserve_exact(changes.len())
        .map_err(|_| AlterPartitionReassignmentsProtocolFailure::RetainedBytes)?;
    for change in changes {
        let index = returned
            .binary_search_by(|partition| {
                partition
                    .topic
                    .as_bytes()
                    .cmp(change.topic().as_bytes())
                    .then_with(|| partition.partition.cmp(&change.partition()))
            })
            .map_err(|_insertion| AlterPartitionReassignmentsProtocolFailure::MissingPartition)?;
        let partition = returned[index];
        match NonZeroI16::new(partition.error_code) {
            None => outcomes.push(AlterPartitionReassignmentOutcome::altered(
                change.topic().to_owned(),
                change.partition(),
            )),
            Some(code) => outcomes.push(AlterPartitionReassignmentOutcome::failed(
                change.topic().to_owned(),
                change.partition(),
                bounded_error(code, partition.error_message),
            )),
        }
    }
    Ok(outcomes)
}

fn expected_order(left: &Expected<'_>, right: &Expected<'_>) -> core::cmp::Ordering {
    left.topic
        .as_bytes()
        .cmp(right.topic.as_bytes())
        .then_with(|| left.partition.cmp(&right.partition))
        .then_with(|| left.caller_index.cmp(&right.caller_index))
}

fn returned_order(left: &Returned<'_>, right: &Returned<'_>) -> core::cmp::Ordering {
    left.topic
        .as_bytes()
        .cmp(right.topic.as_bytes())
        .then_with(|| left.partition.cmp(&right.partition))
        .then_with(|| left.source_topic.cmp(&right.source_topic))
}

fn same_expected(left: Expected<'_>, right: Expected<'_>) -> bool {
    left.topic == right.topic && left.partition == right.partition
}

fn unique_expected_topics(expected: &[Expected<'_>]) -> usize {
    expected
        .iter()
        .enumerate()
        .filter(|(index, entry)| *index == 0 || expected[*index - 1].topic != entry.topic)
        .count()
}

fn validate_returned(
    returned: &[Returned<'_>],
) -> Result<(), AlterPartitionReassignmentsProtocolFailure> {
    if returned.iter().any(|entry| entry.partition < 0) {
        return Err(AlterPartitionReassignmentsProtocolFailure::NegativePartition);
    }
    for pair in returned.windows(2) {
        let [left, right] = pair else {
            continue;
        };
        if left.topic == right.topic && left.source_topic != right.source_topic {
            return Err(AlterPartitionReassignmentsProtocolFailure::DuplicateTopic);
        }
        if left.topic == right.topic && left.partition == right.partition {
            return Err(AlterPartitionReassignmentsProtocolFailure::DuplicatePartition);
        }
    }
    Ok(())
}
