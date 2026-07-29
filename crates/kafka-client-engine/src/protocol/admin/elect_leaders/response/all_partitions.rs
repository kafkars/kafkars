//! Bounded canonical normalization for all-partition election responses.

use core::num::NonZeroI16;

use kafka_client_core::LeaderElectionOutcome;
use kafka_wire::ElectLeadersResponse;

use super::{DIAGNOSTIC_LIMIT, ElectLeadersProtocolFailure, bounded_error};
use crate::protocol::admin::elect_leaders::retention::{
    MAX_RESPONSE_PARTITIONS, MAX_RESPONSE_TOPICS, MAX_TOPIC_NAME_BYTES, all_result_charge,
};

#[derive(Clone, Copy)]
struct Returned<'a> {
    topic: &'a str,
    partition: i32,
    error_code: i16,
    error_message: Option<&'a str>,
    source_topic: usize,
}

pub(super) fn normalize(
    response: &ElectLeadersResponse,
    result_limit: usize,
) -> Result<Vec<LeaderElectionOutcome>, ElectLeadersProtocolFailure> {
    let returned_count = validate_shape(response)?;
    let charge = all_result_charge(response, DIAGNOSTIC_LIMIT)
        .ok_or(ElectLeadersProtocolFailure::RetainedBytes)?;
    if charge > result_limit {
        return Err(ElectLeadersProtocolFailure::RetainedBytes);
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
    validate_identities(&returned)?;
    normalize_returned(&returned)
}

fn validate_shape(response: &ElectLeadersResponse) -> Result<usize, ElectLeadersProtocolFailure> {
    if response.replica_election_results.len() > MAX_RESPONSE_TOPICS {
        return Err(ElectLeadersProtocolFailure::TopicCount);
    }
    let mut partition_count = 0usize;
    for topic in &response.replica_election_results {
        if topic.topic.is_empty() {
            return Err(ElectLeadersProtocolFailure::EmptyTopic);
        }
        if topic.topic.len() > MAX_TOPIC_NAME_BYTES {
            return Err(ElectLeadersProtocolFailure::TopicNameTooLong);
        }
        if topic.partition_result.is_empty() {
            return Err(ElectLeadersProtocolFailure::EmptyTopicPartitions);
        }
        partition_count = partition_count
            .checked_add(topic.partition_result.len())
            .ok_or(ElectLeadersProtocolFailure::PartitionCount)?;
        if partition_count > MAX_RESPONSE_PARTITIONS {
            return Err(ElectLeadersProtocolFailure::PartitionCount);
        }
    }
    Ok(partition_count)
}

fn validate_identities(returned: &[Returned<'_>]) -> Result<(), ElectLeadersProtocolFailure> {
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

fn normalize_returned(
    returned: &[Returned<'_>],
) -> Result<Vec<LeaderElectionOutcome>, ElectLeadersProtocolFailure> {
    let mut outcomes = Vec::new();
    outcomes
        .try_reserve_exact(returned.len())
        .map_err(|_| ElectLeadersProtocolFailure::RetainedBytes)?;
    for partition in returned {
        let topic = copy_topic(partition.topic)?;
        match NonZeroI16::new(partition.error_code) {
            None => outcomes.push(LeaderElectionOutcome::elected(topic, partition.partition)),
            Some(code) => outcomes.push(LeaderElectionOutcome::failed(
                topic,
                partition.partition,
                bounded_error(code, partition.error_message),
            )),
        }
    }
    Ok(outcomes)
}

fn returned_order(left: &Returned<'_>, right: &Returned<'_>) -> core::cmp::Ordering {
    left.topic
        .as_bytes()
        .cmp(right.topic.as_bytes())
        .then_with(|| left.partition.cmp(&right.partition))
        .then_with(|| left.source_topic.cmp(&right.source_topic))
}

fn copy_topic(topic: &str) -> Result<String, ElectLeadersProtocolFailure> {
    let mut copied = String::new();
    copied
        .try_reserve_exact(topic.len())
        .map_err(|_| ElectLeadersProtocolFailure::RetainedBytes)?;
    copied.push_str(topic);
    Ok(copied)
}
