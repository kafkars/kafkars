//! Exhaustive stable translation of engine-owned metadata-quorum outcomes.

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{
        MetadataQuorumDescription as PublicDescription, MetadataQuorumListener as PublicListener,
        MetadataQuorumNode as PublicNode, MetadataQuorumReplica as PublicReplica,
    },
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, BrokerError, DeliveryStatus,
        Description, Failure, FailureKind, Listener, Node, ObserverError, Outcome, PartitionError,
        Replica,
    },
    operation::AdminDescribeMetadataQuorumResult,
};

pub(super) fn translate_admission_error(error: AdmissionError) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(kind: AdmissionErrorKind) -> KafkaError {
    let public = match kind {
        AdmissionErrorKind::InvalidDeadline => ErrorKind::Configuration,
        AdmissionErrorKind::Contended
        | AdmissionErrorKind::Capacity
        | AdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        AdmissionErrorKind::Closed => ErrorKind::State,
        AdmissionErrorKind::IdentityExhausted | AdmissionErrorKind::HostUnavailable => {
            ErrorKind::Internal
        }
    };
    KafkaError::new(
        public,
        format!("DescribeMetadataQuorum admission failed: {kind:?}"),
    )
    .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DescribeMetadataQuorum was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DescribeMetadataQuorum was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminDescribeMetadataQuorumResult {
    match result {
        Ok(Outcome::Described(description)) => Ok(translate_description(description)),
        Ok(Outcome::BrokerRejected(error)) => Err(translate_top_level_broker_error(error)),
        Ok(Outcome::PartitionRejected(error)) => Err(translate_partition_broker_error(error)),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_description(description: Description) -> PublicDescription {
    let (leader_id, leader_epoch, high_watermark, voters, observers, nodes) =
        description.into_parts();
    PublicDescription::new(
        leader_id,
        leader_epoch,
        high_watermark,
        voters.into_iter().map(translate_replica).collect(),
        observers.into_iter().map(translate_replica).collect(),
        nodes.map(|nodes| nodes.into_iter().map(translate_node).collect()),
    )
}

fn translate_replica(replica: Replica) -> PublicReplica {
    let (
        replica_id,
        replica_directory_id,
        log_end_offset,
        last_fetch_timestamp_ms,
        last_caught_up_timestamp_ms,
    ) = replica.into_parts();
    PublicReplica::new(
        replica_id,
        replica_directory_id,
        log_end_offset,
        last_fetch_timestamp_ms,
        last_caught_up_timestamp_ms,
    )
}

fn translate_node(node: Node) -> PublicNode {
    let (node_id, listeners) = node.into_parts();
    PublicNode::new(
        node_id,
        listeners.into_iter().map(translate_listener).collect(),
    )
}

fn translate_listener(listener: Listener) -> PublicListener {
    let (name, host, port) = listener.into_parts();
    PublicListener::new(name, host, port)
}

fn translate_top_level_broker_error(error: BrokerError) -> KafkaError {
    let (code, message, message_truncated) = error.into_parts();
    translate_broker_error_parts(code, message.as_deref(), message_truncated, "top-level")
}

fn translate_partition_broker_error(error: PartitionError) -> KafkaError {
    let (code, message, message_truncated) = error.into_parts();
    translate_broker_error_parts(
        code,
        message.as_deref(),
        message_truncated,
        "metadata-partition",
    )
}

pub(super) fn translate_broker_error_parts(
    code: i16,
    message: Option<&str>,
    message_truncated: bool,
    scope: &str,
) -> KafkaError {
    let detail = message.map_or_else(
        || {
            format!(
                "Kafka rejected DescribeMetadataQuorum at {scope} scope with broker code {code}"
            )
        },
        |message| {
            format!(
                "Kafka rejected DescribeMetadataQuorum at {scope} scope with broker code {code}: {message}"
            )
        },
    );
    KafkaError::new(ErrorKind::Broker, detail)
        .with_broker_code(Some(code))
        .with_delivery_status(PublicDeliveryStatus::PossiblySent)
        .with_diagnostic_truncated(message_truncated)
}

fn translate_failure(failure: Failure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(kind: FailureKind, delivery: DeliveryStatus) -> KafkaError {
    let public = match kind {
        FailureKind::DeadlineElapsed => ErrorKind::Timeout,
        FailureKind::DriverRejected | FailureKind::ResponseTooLarge => ErrorKind::Backpressure,
        FailureKind::Transport => ErrorKind::Transport,
        FailureKind::Compatibility => ErrorKind::Compatibility,
        FailureKind::InvalidResponse => ErrorKind::Broker,
    };
    KafkaError::new(public, format!("DescribeMetadataQuorum failed: {kind:?}"))
        .with_delivery_status(translate_delivery(delivery))
}

const fn translate_delivery(delivery: DeliveryStatus) -> PublicDeliveryStatus {
    match delivery {
        DeliveryStatus::NotSent => PublicDeliveryStatus::NotSent,
        DeliveryStatus::PossiblySent => PublicDeliveryStatus::PossiblySent,
    }
}

pub(super) fn translate_observer_error(error: ObserverError) -> KafkaError {
    let public = match error {
        ObserverError::AlreadyObserved => ErrorKind::State,
        ObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
