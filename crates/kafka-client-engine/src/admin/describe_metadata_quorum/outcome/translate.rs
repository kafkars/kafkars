//! Exhaustive core-to-engine metadata-quorum terminal translation.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, DescribeMetadataQuorumBrokerError as CoreBrokerError,
    DescribeMetadataQuorumDescription as CoreDescription,
    DescribeMetadataQuorumFailureKind as CoreFailureKind,
    DescribeMetadataQuorumListener as CoreListener, DescribeMetadataQuorumNode as CoreNode,
    DescribeMetadataQuorumPartitionError as CorePartitionError,
    DescribeMetadataQuorumReplica as CoreReplica, DescribeMetadataQuorumTerminal as CoreTerminal,
};

use super::super::{
    DescribeMetadataQuorumDescription, DescribeMetadataQuorumListener, DescribeMetadataQuorumNode,
    DescribeMetadataQuorumReplica,
};
use super::{
    DescribeMetadataQuorumBrokerError, DescribeMetadataQuorumDeliveryStatus,
    DescribeMetadataQuorumFailure, DescribeMetadataQuorumFailureKind,
    DescribeMetadataQuorumOutcome, DescribeMetadataQuorumPartitionError,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> DescribeMetadataQuorumOutcome {
    match terminal {
        CoreTerminal::Described(description) => {
            DescribeMetadataQuorumOutcome::Described(translate_description(description))
        }
        CoreTerminal::BrokerRejected(error) => {
            DescribeMetadataQuorumOutcome::BrokerRejected(translate_broker_error(error))
        }
        CoreTerminal::PartitionRejected(error) => {
            DescribeMetadataQuorumOutcome::PartitionRejected(translate_partition_error(error))
        }
        CoreTerminal::Failed(failure) => {
            DescribeMetadataQuorumOutcome::Failed(DescribeMetadataQuorumFailure {
                kind: translate_failure_kind(failure.kind()),
                delivery: translate_delivery(failure.delivery()),
            })
        }
    }
}

fn translate_description(description: CoreDescription) -> DescribeMetadataQuorumDescription {
    let (leader_id, leader_epoch, high_watermark, voters, observers, nodes) =
        description.into_parts();
    DescribeMetadataQuorumDescription {
        leader_id,
        leader_epoch,
        high_watermark,
        voters: voters.into_iter().map(translate_replica).collect(),
        observers: observers.into_iter().map(translate_replica).collect(),
        nodes: nodes.map(|nodes| nodes.into_iter().map(translate_node).collect()),
    }
}

fn translate_replica(replica: CoreReplica) -> DescribeMetadataQuorumReplica {
    let (
        replica_id,
        replica_directory_id,
        log_end_offset,
        last_fetch_timestamp_ms,
        last_caught_up_timestamp_ms,
    ) = replica.into_parts();
    DescribeMetadataQuorumReplica {
        replica_id,
        replica_directory_id,
        log_end_offset,
        last_fetch_timestamp_ms,
        last_caught_up_timestamp_ms,
    }
}

fn translate_node(node: CoreNode) -> DescribeMetadataQuorumNode {
    let (node_id, listeners) = node.into_parts();
    DescribeMetadataQuorumNode {
        node_id,
        listeners: listeners.into_iter().map(translate_listener).collect(),
    }
}

fn translate_listener(listener: CoreListener) -> DescribeMetadataQuorumListener {
    let (name, host, port) = listener.into_parts();
    DescribeMetadataQuorumListener { name, host, port }
}

fn translate_failure_kind(kind: CoreFailureKind) -> DescribeMetadataQuorumFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => DescribeMetadataQuorumFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => DescribeMetadataQuorumFailureKind::DriverRejected,
        CoreFailureKind::Transport => DescribeMetadataQuorumFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => DescribeMetadataQuorumFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => DescribeMetadataQuorumFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => DescribeMetadataQuorumFailureKind::InvalidResponse,
    }
}

fn translate_broker_error(error: CoreBrokerError) -> DescribeMetadataQuorumBrokerError {
    let (code, message, message_truncated) = error.into_parts();
    DescribeMetadataQuorumBrokerError {
        code,
        message,
        message_truncated,
    }
}

fn translate_partition_error(error: CorePartitionError) -> DescribeMetadataQuorumPartitionError {
    let (code, message, message_truncated) = error.into_parts();
    DescribeMetadataQuorumPartitionError {
        code,
        message,
        message_truncated,
    }
}

const fn translate_delivery(status: CoreDeliveryStatus) -> DescribeMetadataQuorumDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => DescribeMetadataQuorumDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => DescribeMetadataQuorumDeliveryStatus::PossiblySent,
    }
}
