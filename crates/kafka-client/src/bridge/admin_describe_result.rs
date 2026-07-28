//! Exhaustive stable translation of concrete engine cluster-description outcomes.

use kafka_client_engine::{
    ClusterBroker as EngineClusterBroker, DescribeClusterAcceptedFaultKind,
    DescribeClusterAdmissionError, DescribeClusterAdmissionErrorKind, DescribeClusterBrokerError,
    DescribeClusterDeliveryStatus, DescribeClusterFailure, DescribeClusterFailureKind,
    DescribeClusterObserverError, DescribeClusterOutcome,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError,
    admin::{ClusterBroker, ClusterDescription},
    bridge::admin_describe_operation::AdminDescribeClusterResult,
};

pub(super) fn translate_admission_error(error: DescribeClusterAdmissionError) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(kind: DescribeClusterAdmissionErrorKind) -> KafkaError {
    let public = match kind {
        DescribeClusterAdmissionErrorKind::InvalidDeadline => ErrorKind::Configuration,
        DescribeClusterAdmissionErrorKind::Contended
        | DescribeClusterAdmissionErrorKind::Capacity
        | DescribeClusterAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        DescribeClusterAdmissionErrorKind::Closed => ErrorKind::State,
        DescribeClusterAdmissionErrorKind::IdentityExhausted
        | DescribeClusterAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    KafkaError::new(
        public,
        format!("DescribeCluster admission failed: {kind:?}"),
    )
    .with_delivery_status(DeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: DescribeClusterAcceptedFaultKind) -> KafkaError {
    match fault {
        DescribeClusterAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DescribeCluster was accepted but its host wake failed",
        ),
        DescribeClusterAcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DescribeCluster was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<DescribeClusterOutcome, DescribeClusterObserverError>,
) -> AdminDescribeClusterResult {
    match result {
        Ok(DescribeClusterOutcome::Cluster(description)) => {
            let (cluster_id, controller_id, brokers, authorized_operations) =
                description.into_parts_with_authorized_operations();
            Ok(translate_description_parts(
                cluster_id,
                controller_id,
                brokers,
                authorized_operations,
            ))
        }
        Ok(DescribeClusterOutcome::BrokerRejected(error)) => Err(translate_broker_error(error)),
        Ok(DescribeClusterOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

pub(super) fn translate_description_parts(
    cluster_id: String,
    controller_id: Option<i32>,
    brokers: Vec<EngineClusterBroker>,
    authorized_operations: Option<i32>,
) -> ClusterDescription {
    ClusterDescription::new_with_authorized_operations(
        cluster_id,
        controller_id,
        brokers
            .into_iter()
            .map(|broker| {
                let (id, host, port, rack, fenced) = broker.into_parts();
                ClusterBroker::new(id, host, port, rack, fenced)
            })
            .collect(),
        authorized_operations,
    )
}

fn translate_broker_error(error: DescribeClusterBrokerError) -> KafkaError {
    let (code, message, message_truncated) = error.into_parts();
    translate_broker_error_parts(code, message.as_deref(), message_truncated)
}

pub(super) fn translate_broker_error_parts(
    code: i16,
    message: Option<&str>,
    message_truncated: bool,
) -> KafkaError {
    let detail = message.map_or_else(
        || format!("Kafka rejected DescribeCluster with broker code {code}"),
        |message| format!("Kafka rejected DescribeCluster with broker code {code}: {message}"),
    );
    KafkaError::new(ErrorKind::Broker, detail)
        .with_broker_code(Some(code))
        .with_delivery_status(DeliveryStatus::PossiblySent)
        .with_diagnostic_truncated(message_truncated)
}

fn translate_failure(failure: DescribeClusterFailure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(
    kind: DescribeClusterFailureKind,
    delivery: DescribeClusterDeliveryStatus,
) -> KafkaError {
    let public = match kind {
        DescribeClusterFailureKind::DeadlineElapsed => ErrorKind::Timeout,
        DescribeClusterFailureKind::DriverRejected => ErrorKind::Backpressure,
        DescribeClusterFailureKind::Transport => ErrorKind::Transport,
        DescribeClusterFailureKind::Compatibility => ErrorKind::Compatibility,
        DescribeClusterFailureKind::Authentication => ErrorKind::Access,
        DescribeClusterFailureKind::InvalidResponse => ErrorKind::Broker,
    };
    KafkaError::new(public, format!("DescribeCluster failed: {kind:?}"))
        .with_delivery_status(translate_delivery(delivery))
}

const fn translate_delivery(delivery: DescribeClusterDeliveryStatus) -> DeliveryStatus {
    match delivery {
        DescribeClusterDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        DescribeClusterDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

pub(super) fn translate_observer_error(error: DescribeClusterObserverError) -> KafkaError {
    let public = match error {
        DescribeClusterObserverError::AlreadyObserved => ErrorKind::State,
        DescribeClusterObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
