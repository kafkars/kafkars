//! Allocation-free selected-version and hostile-shape validation.

use kafka_wire::{
    DescribeQuorumResponse,
    describe_quorum_response::{Node, PartitionData, ReplicaState},
};

use super::{
    request::{METADATA_PARTITION, METADATA_TOPIC},
    response::DescribeMetadataQuorumProtocolFailure,
};

pub(super) const MIN_VERSION: i16 = 0;
pub(super) const MAX_VERSION: i16 = 2;
const MAX_REPLICAS_PER_ROLE: usize = 1024;
const MAX_NODES: usize = 1024;
const MAX_LISTENERS_PER_NODE: usize = 64;
const MAX_STRING_BYTES: usize = i16::MAX as usize;

pub(super) fn successful_partition(
    version: i16,
    response: &DescribeQuorumResponse,
) -> Result<&PartitionData, DescribeMetadataQuorumProtocolFailure> {
    if response.topics.len() != 1 {
        return Err(
            DescribeMetadataQuorumProtocolFailure::UnexpectedTopicCount {
                actual: response.topics.len(),
            },
        );
    }
    let topic = &response.topics[0];
    if topic.topic_name.as_str() != METADATA_TOPIC {
        return Err(DescribeMetadataQuorumProtocolFailure::UnexpectedTopicName);
    }
    if topic.partitions.len() != 1 {
        return Err(
            DescribeMetadataQuorumProtocolFailure::UnexpectedPartitionCount {
                actual: topic.partitions.len(),
            },
        );
    }
    let partition = &topic.partitions[0];
    if partition.partition_index != METADATA_PARTITION {
        return Err(DescribeMetadataQuorumProtocolFailure::UnexpectedPartition {
            actual: partition.partition_index,
        });
    }
    if version < 2 && !response.nodes.is_empty() {
        return Err(
            DescribeMetadataQuorumProtocolFailure::FieldNotRepresentable { field: "nodes" },
        );
    }
    Ok(partition)
}

pub(super) fn validate_success_payload(
    version: i16,
    partition: &PartitionData,
    nodes: &[Node],
) -> Result<(), DescribeMetadataQuorumProtocolFailure> {
    count(
        "current_voters",
        partition.current_voters.len(),
        MAX_REPLICAS_PER_ROLE,
    )?;
    count(
        "observers",
        partition.observers.len(),
        MAX_REPLICAS_PER_ROLE,
    )?;
    if partition.current_voters.is_empty() {
        return Err(DescribeMetadataQuorumProtocolFailure::EmptyVoterSet);
    }
    count("nodes", nodes.len(), MAX_NODES)?;
    for replica in partition.current_voters.iter().chain(&partition.observers) {
        validate_replica(version, replica)?;
    }
    optional_i32("leader_id", partition.leader_id)?;
    nonnegative_i32("leader_epoch", partition.leader_epoch)?;
    nonnegative_i64("high_watermark", partition.high_watermark)?;
    if partition.leader_id >= 0
        && !partition
            .current_voters
            .iter()
            .any(|replica| replica.replica_id == partition.leader_id)
    {
        return Err(DescribeMetadataQuorumProtocolFailure::LeaderNotVoter {
            actual: partition.leader_id,
        });
    }
    for node in nodes {
        validate_node(node)?;
    }
    Ok(())
}

fn validate_replica(
    version: i16,
    replica: &ReplicaState,
) -> Result<(), DescribeMetadataQuorumProtocolFailure> {
    if replica.replica_id < 0 {
        return Err(DescribeMetadataQuorumProtocolFailure::NegativeId {
            field: "replica_id",
            actual: replica.replica_id,
        });
    }
    optional_i64("log_end_offset", replica.log_end_offset)?;
    if version == 0 {
        if replica.last_fetch_timestamp != -1 || replica.last_caught_up_timestamp != -1 {
            return Err(
                DescribeMetadataQuorumProtocolFailure::FieldNotRepresentable {
                    field: "replica timestamps",
                },
            );
        }
    } else {
        optional_i64("last_fetch_timestamp", replica.last_fetch_timestamp)?;
        optional_i64("last_caught_up_timestamp", replica.last_caught_up_timestamp)?;
    }
    if version < 2 && !replica.replica_directory_id.is_zero() {
        return Err(
            DescribeMetadataQuorumProtocolFailure::FieldNotRepresentable {
                field: "replica_directory_id",
            },
        );
    }
    Ok(())
}

fn validate_node(node: &Node) -> Result<(), DescribeMetadataQuorumProtocolFailure> {
    if node.node_id < 0 {
        return Err(DescribeMetadataQuorumProtocolFailure::NegativeId {
            field: "node_id",
            actual: node.node_id,
        });
    }
    count("listeners", node.listeners.len(), MAX_LISTENERS_PER_NODE)?;
    for listener in &node.listeners {
        for (field, value) in [
            ("listener_name", listener.name.as_str()),
            ("listener_host", listener.host.as_str()),
        ] {
            if value.is_empty() {
                return Err(DescribeMetadataQuorumProtocolFailure::EmptyString { field });
            }
            if value.len() > MAX_STRING_BYTES {
                return Err(DescribeMetadataQuorumProtocolFailure::StringTooLong {
                    field,
                    actual: value.len(),
                    max: MAX_STRING_BYTES,
                });
            }
        }
        if listener.port == 0 {
            return Err(DescribeMetadataQuorumProtocolFailure::ZeroListenerPort);
        }
    }
    Ok(())
}

pub(super) fn optional_i32(
    field: &'static str,
    value: i32,
) -> Result<Option<i32>, DescribeMetadataQuorumProtocolFailure> {
    match value {
        -1 => Ok(None),
        0.. => Ok(Some(value)),
        _ => Err(DescribeMetadataQuorumProtocolFailure::InvalidSentinel {
            field,
            actual: i64::from(value),
        }),
    }
}

pub(super) fn optional_i64(
    field: &'static str,
    value: i64,
) -> Result<Option<i64>, DescribeMetadataQuorumProtocolFailure> {
    match value {
        -1 => Ok(None),
        0.. => Ok(Some(value)),
        _ => Err(DescribeMetadataQuorumProtocolFailure::InvalidSentinel {
            field,
            actual: value,
        }),
    }
}

pub(super) fn nonnegative_i32(
    field: &'static str,
    value: i32,
) -> Result<i32, DescribeMetadataQuorumProtocolFailure> {
    (value >= 0)
        .then_some(value)
        .ok_or(DescribeMetadataQuorumProtocolFailure::InvalidSentinel {
            field,
            actual: i64::from(value),
        })
}

pub(super) fn nonnegative_i64(
    field: &'static str,
    value: i64,
) -> Result<i64, DescribeMetadataQuorumProtocolFailure> {
    (value >= 0)
        .then_some(value)
        .ok_or(DescribeMetadataQuorumProtocolFailure::InvalidSentinel {
            field,
            actual: value,
        })
}

fn count(
    field: &'static str,
    actual: usize,
    max: usize,
) -> Result<(), DescribeMetadataQuorumProtocolFailure> {
    (actual <= max)
        .then_some(())
        .ok_or(DescribeMetadataQuorumProtocolFailure::TooMany { field, actual, max })
}
