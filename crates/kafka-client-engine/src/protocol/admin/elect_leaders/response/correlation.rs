//! Structural correlation and caller-order normalization for election results.

use core::num::NonZeroI16;

use kafka_client_core::LeaderElectionOutcome;
use kafka_wire::ElectLeadersResponse;

use super::{DIAGNOSTIC_LIMIT, ElectLeadersProtocolFailure, LeaderElectionRef, bounded_error};

#[derive(Clone, Copy)]
struct Expected<'a> {
    topic: &'a str,
    partition: i32,
    caller_index: usize,
}

#[derive(Clone, Copy)]
struct Returned<'a> {
    topic: &'a str,
    partition: i32,
    error_code: i16,
    error_message: Option<&'a str>,
    source_topic: usize,
}

pub(super) struct CorrelatedResponse<'a> {
    returned: Vec<Returned<'a>>,
}

impl CorrelatedResponse<'_> {
    pub(super) fn diagnostic_bytes(&self) -> Result<usize, ElectLeadersProtocolFailure> {
        self.returned
            .iter()
            .try_fold(0usize, |bytes, partition| {
                bytes.checked_add(
                    partition
                        .error_message
                        .map_or(0, |message| message.len().min(DIAGNOSTIC_LIMIT)),
                )
            })
            .ok_or(ElectLeadersProtocolFailure::RetainedBytes)
    }

    pub(super) fn normalize(
        &self,
        targets: &[LeaderElectionRef<'_>],
    ) -> Result<Vec<LeaderElectionOutcome>, ElectLeadersProtocolFailure> {
        let mut outcomes = Vec::new();
        outcomes
            .try_reserve_exact(targets.len())
            .map_err(|_| ElectLeadersProtocolFailure::RetainedBytes)?;
        for change in targets {
            let index = self
                .returned
                .binary_search_by(|partition| {
                    partition
                        .topic
                        .as_bytes()
                        .cmp(change.topic().as_bytes())
                        .then_with(|| partition.partition.cmp(&change.partition()))
                })
                .map_err(|_insertion| ElectLeadersProtocolFailure::MissingPartition)?;
            let partition = self.returned[index];
            match NonZeroI16::new(partition.error_code) {
                None => outcomes.push(LeaderElectionOutcome::elected(
                    change.topic().to_owned(),
                    change.partition(),
                )),
                Some(code) => outcomes.push(LeaderElectionOutcome::failed(
                    change.topic().to_owned(),
                    change.partition(),
                    bounded_error(code, partition.error_message),
                )),
            }
        }
        Ok(outcomes)
    }
}

pub(super) fn correlate_response<'response>(
    targets: &[LeaderElectionRef<'_>],
    response: &'response ElectLeadersResponse,
) -> Result<CorrelatedResponse<'response>, ElectLeadersProtocolFailure> {
    let mut expected = Vec::new();
    expected
        .try_reserve_exact(targets.len())
        .map_err(|_| ElectLeadersProtocolFailure::RetainedBytes)?;
    expected.extend(
        targets
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
        return Err(ElectLeadersProtocolFailure::DuplicatePartition);
    }
    if unique_expected_topics(&expected) != response.replica_election_results.len() {
        return Err(ElectLeadersProtocolFailure::TopicCount);
    }

    let returned_count = response
        .replica_election_results
        .iter()
        .try_fold(0usize, |count, topic| {
            count.checked_add(topic.partition_result.len())
        })
        .ok_or(ElectLeadersProtocolFailure::PartitionCount)?;
    if returned_count != targets.len() {
        return Err(ElectLeadersProtocolFailure::PartitionCount);
    }
    let mut returned = Vec::new();
    returned
        .try_reserve_exact(returned_count)
        .map_err(|_| ElectLeadersProtocolFailure::RetainedBytes)?;
    for (source_topic, topic) in response.replica_election_results.iter().enumerate() {
        returned.extend(topic.partition_result.iter().map(|partition| {
            Returned {
                topic: topic.topic.as_str(),
                partition: partition.partition_id,
                error_code: partition.error_code,
                error_message: partition
                    .error_message
                    .as_ref()
                    .map(kafka_wire_core::StrBytes::as_str),
                source_topic,
            }
        }));
    }
    returned.sort_unstable_by(returned_order);
    validate_returned(&returned)?;

    for (expected, returned) in expected.iter().zip(&returned) {
        match returned.topic.as_bytes().cmp(expected.topic.as_bytes()) {
            core::cmp::Ordering::Less => {
                return Err(ElectLeadersProtocolFailure::UnexpectedTopic);
            }
            core::cmp::Ordering::Greater => {
                return Err(ElectLeadersProtocolFailure::MissingTopic);
            }
            core::cmp::Ordering::Equal => {}
        }
        match returned.partition.cmp(&expected.partition) {
            core::cmp::Ordering::Less => {
                return Err(ElectLeadersProtocolFailure::UnexpectedPartition);
            }
            core::cmp::Ordering::Greater => {
                return Err(ElectLeadersProtocolFailure::MissingPartition);
            }
            core::cmp::Ordering::Equal => {}
        }
    }
    Ok(CorrelatedResponse { returned })
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

fn validate_returned(returned: &[Returned<'_>]) -> Result<(), ElectLeadersProtocolFailure> {
    if returned.iter().any(|entry| entry.partition < 0) {
        return Err(ElectLeadersProtocolFailure::NegativePartition);
    }
    for pair in returned.windows(2) {
        let [left, right] = pair else {
            continue;
        };
        if left.topic == right.topic && left.source_topic != right.source_topic {
            return Err(ElectLeadersProtocolFailure::DuplicateTopic);
        }
        if left.topic == right.topic && left.partition == right.partition {
            return Err(ElectLeadersProtocolFailure::DuplicatePartition);
        }
    }
    Ok(())
}
