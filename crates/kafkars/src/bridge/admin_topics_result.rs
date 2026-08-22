//! Exhaustive stable translation of concrete engine topic descriptions.

use kafka_client_engine::{
    DescribeTopicError as EngineTopicError, DescribeTopicsAcceptedFaultKind,
    DescribeTopicsAdmissionError, DescribeTopicsAdmissionErrorKind, DescribeTopicsDeliveryStatus,
    DescribeTopicsFailure, DescribeTopicsFailureKind, DescribeTopicsObserverError,
    DescribeTopicsOutcome, TopicDescription as EngineTopicDescription,
    TopicPartitionDescription as EnginePartitionDescription,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError,
    admin::{BatchResult, TopicDescription, TopicPartitionDescription},
    bridge::admin_topics_operation::AdminDescribeTopicsResult,
};

pub(in crate::bridge) fn translate_admission_error(
    error: DescribeTopicsAdmissionError,
) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(kind: DescribeTopicsAdmissionErrorKind) -> KafkaError {
    let public = match kind {
        DescribeTopicsAdmissionErrorKind::InvalidRequest
        | DescribeTopicsAdmissionErrorKind::InvalidDeadline => ErrorKind::Configuration,
        DescribeTopicsAdmissionErrorKind::Contended
        | DescribeTopicsAdmissionErrorKind::Capacity
        | DescribeTopicsAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        DescribeTopicsAdmissionErrorKind::Closed => ErrorKind::State,
        DescribeTopicsAdmissionErrorKind::IdentityExhausted
        | DescribeTopicsAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    let error = KafkaError::new(public, format!("DescribeTopics admission failed: {kind:?}"))
        .with_delivery_status(DeliveryStatus::NotSent);
    match kind {
        DescribeTopicsAdmissionErrorKind::Contended
        | DescribeTopicsAdmissionErrorKind::Capacity
        | DescribeTopicsAdmissionErrorKind::RetainedBytes => error.with_safe_retry(),
        DescribeTopicsAdmissionErrorKind::InvalidRequest
        | DescribeTopicsAdmissionErrorKind::InvalidDeadline
        | DescribeTopicsAdmissionErrorKind::Closed
        | DescribeTopicsAdmissionErrorKind::IdentityExhausted
        | DescribeTopicsAdmissionErrorKind::HostUnavailable => error,
    }
}

pub(in crate::bridge) fn translate_accepted_fault(
    fault: DescribeTopicsAcceptedFaultKind,
) -> KafkaError {
    match fault {
        DescribeTopicsAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DescribeTopics was accepted but its host wake failed",
        ),
        DescribeTopicsAcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DescribeTopics was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<DescribeTopicsOutcome, DescribeTopicsObserverError>,
) -> AdminDescribeTopicsResult {
    match result {
        Ok(DescribeTopicsOutcome::Topics(topics)) => Ok(BatchResult::new(
            topics
                .into_iter()
                .map(|topic| {
                    let (name, internal, result) = topic.into_parts();
                    (
                        name,
                        result
                            .map(translate_description)
                            .map_err(|error| translate_topic_error(error, internal)),
                    )
                })
                .collect(),
        )),
        Ok(DescribeTopicsOutcome::TopicIds(_)) => Err(KafkaError::new(
            ErrorKind::Internal,
            "name-based DescribeTopics received a topic-ID terminal",
        )
        .with_delivery_status(DeliveryStatus::PossiblySent)),
        Ok(DescribeTopicsOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

pub(in crate::bridge) fn translate_description(
    description: EngineTopicDescription,
) -> TopicDescription {
    let (name, topic_id, internal, partitions, authorized_operations) = description.into_parts();
    TopicDescription::new(
        name,
        topic_id,
        internal,
        partitions.into_iter().map(translate_partition).collect(),
    )
    .with_authorized_operations(authorized_operations)
}

fn translate_partition(partition: EnginePartitionDescription) -> TopicPartitionDescription {
    let (index, error_code, leader, epoch, replicas, isr, offline) = partition.into_parts();
    TopicPartitionDescription::new(
        index,
        error_code.map(partition_error),
        leader,
        epoch,
        replicas,
        isr,
        offline,
    )
}

pub(in crate::bridge) fn translate_failure(failure: DescribeTopicsFailure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(
    failure: DescribeTopicsFailureKind,
    delivery: DescribeTopicsDeliveryStatus,
) -> KafkaError {
    let (kind, broker_code) = match failure {
        DescribeTopicsFailureKind::DeadlineElapsed => (ErrorKind::Timeout, None),
        DescribeTopicsFailureKind::DriverRejected | DescribeTopicsFailureKind::ResponseTooLarge => {
            (ErrorKind::Backpressure, None)
        }
        DescribeTopicsFailureKind::Transport => (ErrorKind::Transport, None),
        DescribeTopicsFailureKind::Broker(code) => (ErrorKind::Broker, Some(code)),
        DescribeTopicsFailureKind::Compatibility => (ErrorKind::Compatibility, None),
        DescribeTopicsFailureKind::InvalidResponse => (ErrorKind::Broker, None),
    };
    let detail = match failure {
        DescribeTopicsFailureKind::ResponseTooLarge => {
            "DescribeTopics response exceeded its admitted retained-result budget".to_owned()
        }
        DescribeTopicsFailureKind::Compatibility => {
            "broker cannot represent DescribeTopics read-only query policy".to_owned()
        }
        _ => format!("DescribeTopics failed: {failure:?}"),
    };
    KafkaError::new(kind, detail)
        .with_broker_code(broker_code)
        .with_delivery_status(translate_delivery(delivery))
}

pub(in crate::bridge) fn translate_topic_error(
    error: EngineTopicError,
    internal: bool,
) -> KafkaError {
    topic_error(error.code(), internal)
}

pub(super) fn topic_error(code: i16, internal: bool) -> KafkaError {
    partition_error(code).with_internal_topic(internal)
}

pub(super) fn partition_error(code: i16) -> KafkaError {
    KafkaError::new(
        ErrorKind::Broker,
        format!("Kafka returned Metadata broker code {code}"),
    )
    .with_broker_code(Some(code))
}

const fn translate_delivery(delivery: DescribeTopicsDeliveryStatus) -> DeliveryStatus {
    match delivery {
        DescribeTopicsDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        DescribeTopicsDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

pub(in crate::bridge) fn translate_observer_error(
    error: DescribeTopicsObserverError,
) -> KafkaError {
    let public = match error {
        DescribeTopicsObserverError::AlreadyObserved => ErrorKind::State,
        DescribeTopicsObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
