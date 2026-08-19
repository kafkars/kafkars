//! Exhaustive normalized-protocol translation into deterministic core input.

use core::num::NonZeroI16;

use kafka_client_core::{
    DeliveryStatus, DescribeMetadataQuorumBrokerError, DescribeMetadataQuorumDescription,
    DescribeMetadataQuorumInput, DescribeMetadataQuorumListener, DescribeMetadataQuorumNode,
    DescribeMetadataQuorumPartitionError, DescribeMetadataQuorumReplica,
};

use crate::{
    driver::{
        DescribeMetadataQuorumDriverFailureKind, DescribeMetadataQuorumRawTerminal,
        DescribeMetadataQuorumTerminalFact,
    },
    protocol::admin::describe_metadata_quorum::{
        DescribeMetadataQuorumProtocolFailure, NormalizedMetadataQuorum,
        NormalizedMetadataQuorumOutcome, NormalizedQuorumError, NormalizedQuorumListener,
        NormalizedQuorumNode, NormalizedQuorumReplica, normalize_describe_metadata_quorum_response,
    },
};

pub(super) fn terminal_input(
    raw: &DescribeMetadataQuorumRawTerminal,
    retained_limit: usize,
) -> (DescribeMetadataQuorumInput, usize) {
    match raw.fact() {
        DescribeMetadataQuorumTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => match normalize_describe_metadata_quorum_response(
            selected_version,
            response,
            retained_limit,
        ) {
            Ok(normalized) => {
                let (outcome, retained_bytes) = normalized.into_parts();
                (normalized_input(outcome), retained_bytes)
            }
            Err(error) => (protocol_failure(error), 0),
        },
        DescribeMetadataQuorumTerminalFact::Response {
            selected_version: None,
            ..
        } => (
            DescribeMetadataQuorumInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            0,
        ),
        DescribeMetadataQuorumTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

fn normalized_input(outcome: NormalizedMetadataQuorumOutcome) -> DescribeMetadataQuorumInput {
    match outcome {
        NormalizedMetadataQuorumOutcome::TopLevelError(error) => exact_error(error)
            .map_or(DescribeMetadataQuorumInput::InvalidResponse, |error| {
                DescribeMetadataQuorumInput::BrokerRejected { error }
            }),
        NormalizedMetadataQuorumOutcome::PartitionError(error) => exact_partition_error(error)
            .map_or(DescribeMetadataQuorumInput::InvalidResponse, |error| {
                DescribeMetadataQuorumInput::PartitionRejected { error }
            }),
        NormalizedMetadataQuorumOutcome::Quorum(quorum) => core_description(quorum)
            .map(|description| DescribeMetadataQuorumInput::BrokerResponded { description })
            .unwrap_or(DescribeMetadataQuorumInput::InvalidResponse),
    }
}

fn exact_error(error: NormalizedQuorumError) -> Option<DescribeMetadataQuorumBrokerError> {
    let (code, message, message_truncated) = error.into_parts();
    Some(DescribeMetadataQuorumBrokerError::new(
        NonZeroI16::new(code)?,
        message,
        message_truncated,
    ))
}

fn exact_partition_error(
    error: NormalizedQuorumError,
) -> Option<DescribeMetadataQuorumPartitionError> {
    let (code, message, message_truncated) = error.into_parts();
    Some(DescribeMetadataQuorumPartitionError::new(
        NonZeroI16::new(code)?,
        message,
        message_truncated,
    ))
}

fn core_description(
    quorum: NormalizedMetadataQuorum,
) -> Result<DescribeMetadataQuorumDescription, kafka_client_core::DescribeMetadataQuorumValueError>
{
    let (leader_id, leader_epoch, high_watermark, voters, observers, nodes) = quorum.into_parts();
    DescribeMetadataQuorumDescription::new(
        leader_id,
        leader_epoch,
        high_watermark,
        voters.into_iter().map(core_replica).collect(),
        observers.into_iter().map(core_replica).collect(),
        nodes.map(|nodes| nodes.into_iter().map(core_node).collect()),
    )
}

fn core_replica(replica: NormalizedQuorumReplica) -> DescribeMetadataQuorumReplica {
    let (id, directory, offset, fetched_at, caught_up_at) = replica.into_parts();
    DescribeMetadataQuorumReplica::new(id, directory, offset, fetched_at, caught_up_at)
}

fn core_node(node: NormalizedQuorumNode) -> DescribeMetadataQuorumNode {
    let (id, listeners) = node.into_parts();
    DescribeMetadataQuorumNode::new(id, listeners.into_iter().map(core_listener).collect())
}

fn core_listener(listener: NormalizedQuorumListener) -> DescribeMetadataQuorumListener {
    let (name, host, port) = listener.into_parts();
    DescribeMetadataQuorumListener::new(name, host, port)
}

pub(super) const fn protocol_failure(
    error: DescribeMetadataQuorumProtocolFailure,
) -> DescribeMetadataQuorumInput {
    match error {
        DescribeMetadataQuorumProtocolFailure::UnsupportedApiVersion { .. } => {
            DescribeMetadataQuorumInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        DescribeMetadataQuorumProtocolFailure::RetainedBytes { .. } => {
            DescribeMetadataQuorumInput::ResponseTooLarge
        }
        DescribeMetadataQuorumProtocolFailure::UnexpectedTopicCount { .. }
        | DescribeMetadataQuorumProtocolFailure::UnexpectedTopicName
        | DescribeMetadataQuorumProtocolFailure::UnexpectedPartitionCount { .. }
        | DescribeMetadataQuorumProtocolFailure::UnexpectedPartition { .. }
        | DescribeMetadataQuorumProtocolFailure::FieldNotRepresentable { .. }
        | DescribeMetadataQuorumProtocolFailure::TooMany { .. }
        | DescribeMetadataQuorumProtocolFailure::EmptyVoterSet
        | DescribeMetadataQuorumProtocolFailure::NegativeId { .. }
        | DescribeMetadataQuorumProtocolFailure::InvalidSentinel { .. }
        | DescribeMetadataQuorumProtocolFailure::LeaderNotVoter { .. }
        | DescribeMetadataQuorumProtocolFailure::EmptyString { .. }
        | DescribeMetadataQuorumProtocolFailure::StringTooLong { .. }
        | DescribeMetadataQuorumProtocolFailure::ZeroListenerPort
        | DescribeMetadataQuorumProtocolFailure::DuplicateReplicaId { .. }
        | DescribeMetadataQuorumProtocolFailure::ReplicaInBothRoles { .. }
        | DescribeMetadataQuorumProtocolFailure::DuplicateNodeId { .. }
        | DescribeMetadataQuorumProtocolFailure::DuplicateListenerName { .. } => {
            DescribeMetadataQuorumInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: DescribeMetadataQuorumDriverFailureKind,
    delivery: DeliveryStatus,
) -> DescribeMetadataQuorumInput {
    match kind {
        DescribeMetadataQuorumDriverFailureKind::DeadlineElapsed => {
            DescribeMetadataQuorumInput::DriverDeadlineElapsed { delivery }
        }
        DescribeMetadataQuorumDriverFailureKind::Compatibility => {
            DescribeMetadataQuorumInput::ProtocolIncompatible { delivery }
        }
        DescribeMetadataQuorumDriverFailureKind::InvalidResponse => {
            DescribeMetadataQuorumInput::InvalidResponse
        }
        DescribeMetadataQuorumDriverFailureKind::Transport => {
            DescribeMetadataQuorumInput::TransportFailed { delivery }
        }
    }
}
