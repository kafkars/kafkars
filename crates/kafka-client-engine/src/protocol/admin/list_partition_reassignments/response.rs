//! Bounded validation and deterministic ordering for generated v0 responses.

mod correlation;
#[cfg(test)]
mod correlation_test;

use core::num::NonZeroI16;

use kafka_client_core::{
    LIST_PARTITION_REASSIGNMENTS_DIAGNOSTIC_BYTES, ListPartitionReassignmentsBatch,
    ListPartitionReassignmentsBrokerError, ListPartitionReassignmentsInput,
    ListPartitionReassignmentsPlan, ListPartitionReassignmentsSelection,
};
use kafka_wire::{
    ListPartitionReassignmentsResponse,
    list_partition_reassignments_response::OngoingPartitionReassignment,
};

use super::retention::{broker_error_result_charge, successful_result_charge};

/// Generated response facts unsafe to bind to a reassignment query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListPartitionReassignmentsProtocolFailure {
    UnsupportedApiVersion { actual: i16 },
    NegativeThrottleTime { actual: i32 },
    EmptyTopic,
    EmptyTopicPartitions,
    DuplicateTopic,
    NegativePartition { actual: i32 },
    DuplicatePartition { actual: i32 },
    UnexpectedPartition { actual: i32 },
    EmptyReplicaSet { partition: i32 },
    NegativeBrokerId { actual: i32 },
    DuplicateBrokerId { actual: i32 },
    ConflictingBrokerId { actual: i32 },
    RetainedBytes,
}

/// Converts one generated response into an ordered deterministic core fact.
pub(crate) fn normalize_list_partition_reassignments_response(
    plan: &ListPartitionReassignmentsPlan,
    response: &ListPartitionReassignmentsResponse,
    selected_version: i16,
    result_limit: usize,
) -> Result<ListPartitionReassignmentsInput, ListPartitionReassignmentsProtocolFailure> {
    if selected_version != 0 {
        return Err(
            ListPartitionReassignmentsProtocolFailure::UnsupportedApiVersion {
                actual: selected_version,
            },
        );
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        ListPartitionReassignmentsProtocolFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    if let Some(code) = NonZeroI16::new(response.error_code) {
        let (message, message_truncated) = bounded_diagnostic(
            response
                .error_message
                .as_ref()
                .map(kafka_wire_core::StrBytes::as_str),
        );
        let charge = broker_error_result_charge(message.map_or(0, str::len))
            .ok_or(ListPartitionReassignmentsProtocolFailure::RetainedBytes)?;
        ensure_limit(charge, result_limit)?;
        return Ok(ListPartitionReassignmentsInput::BrokerRejected {
            error: ListPartitionReassignmentsBrokerError::new(
                code,
                message.map(str::to_owned),
                message_truncated,
            ),
        });
    }

    validate_shape(plan.selection(), response)?;
    let selected_target_count = match plan.selection() {
        ListPartitionReassignmentsSelection::Selected(targets) => targets.len(),
        ListPartitionReassignmentsSelection::AllActive => 0,
    };
    let (_row_count, charge) = successful_result_charge(
        response.topics.iter().flat_map(|topic| {
            topic.partitions.iter().map(|partition| {
                (
                    topic.name.as_str(),
                    partition.replicas.len(),
                    partition.adding_replicas.len(),
                    partition.removing_replicas.len(),
                )
            })
        }),
        response.topics.len(),
        selected_target_count,
    )
    .ok_or(ListPartitionReassignmentsProtocolFailure::RetainedBytes)?;
    ensure_limit(charge, result_limit)?;
    let reassignments = correlation::normalize_rows(plan.selection(), response);
    Ok(ListPartitionReassignmentsInput::BrokerResponded {
        batch: ListPartitionReassignmentsBatch::new(throttle_time_ms, reassignments),
    })
}

fn validate_shape(
    selection: &ListPartitionReassignmentsSelection,
    response: &ListPartitionReassignmentsResponse,
) -> Result<(), ListPartitionReassignmentsProtocolFailure> {
    for (topic_index, topic) in response.topics.iter().enumerate() {
        let name = topic.name.as_str();
        if name.is_empty() {
            return Err(ListPartitionReassignmentsProtocolFailure::EmptyTopic);
        }
        if topic.partitions.is_empty() {
            return Err(ListPartitionReassignmentsProtocolFailure::EmptyTopicPartitions);
        }
        if response.topics[..topic_index]
            .iter()
            .any(|earlier| earlier.name.as_str() == name)
        {
            return Err(ListPartitionReassignmentsProtocolFailure::DuplicateTopic);
        }
        for (partition_index, partition) in topic.partitions.iter().enumerate() {
            if partition.partition_index < 0 {
                return Err(
                    ListPartitionReassignmentsProtocolFailure::NegativePartition {
                        actual: partition.partition_index,
                    },
                );
            }
            if topic.partitions[..partition_index]
                .iter()
                .any(|earlier| earlier.partition_index == partition.partition_index)
            {
                return Err(
                    ListPartitionReassignmentsProtocolFailure::DuplicatePartition {
                        actual: partition.partition_index,
                    },
                );
            }
            if let ListPartitionReassignmentsSelection::Selected(targets) = selection {
                if !targets.iter().any(|target| {
                    target.topic() == name && target.partition() == partition.partition_index
                }) {
                    return Err(
                        ListPartitionReassignmentsProtocolFailure::UnexpectedPartition {
                            actual: partition.partition_index,
                        },
                    );
                }
            }
            validate_reassignment(partition)?;
        }
    }
    Ok(())
}

fn validate_reassignment(
    partition: &OngoingPartitionReassignment,
) -> Result<(), ListPartitionReassignmentsProtocolFailure> {
    if partition.replicas.is_empty() {
        return Err(ListPartitionReassignmentsProtocolFailure::EmptyReplicaSet {
            partition: partition.partition_index,
        });
    }
    for brokers in [
        partition.replicas.as_slice(),
        partition.adding_replicas.as_slice(),
        partition.removing_replicas.as_slice(),
    ] {
        for (index, broker) in brokers.iter().enumerate() {
            if *broker < 0 {
                return Err(
                    ListPartitionReassignmentsProtocolFailure::NegativeBrokerId { actual: *broker },
                );
            }
            if brokers[..index].iter().any(|earlier| earlier == broker) {
                return Err(
                    ListPartitionReassignmentsProtocolFailure::DuplicateBrokerId {
                        actual: *broker,
                    },
                );
            }
        }
    }
    if let Some(broker) = partition
        .adding_replicas
        .iter()
        .find(|broker| partition.removing_replicas.contains(broker))
    {
        return Err(
            ListPartitionReassignmentsProtocolFailure::ConflictingBrokerId { actual: *broker },
        );
    }
    Ok(())
}

fn bounded_diagnostic(message: Option<&str>) -> (Option<&str>, bool) {
    let Some(message) = message else {
        return (None, false);
    };
    if message.len() <= LIST_PARTITION_REASSIGNMENTS_DIAGNOSTIC_BYTES {
        return (Some(message), false);
    }
    let mut end = LIST_PARTITION_REASSIGNMENTS_DIAGNOSTIC_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    (Some(&message[..end]), true)
}

fn ensure_limit(
    charge: usize,
    limit: usize,
) -> Result<(), ListPartitionReassignmentsProtocolFailure> {
    (charge <= limit)
        .then_some(())
        .ok_or(ListPartitionReassignmentsProtocolFailure::RetainedBytes)
}
