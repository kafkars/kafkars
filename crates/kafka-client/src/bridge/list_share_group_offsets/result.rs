//! Exhaustive stable translation of engine ShareGroup offset-listing outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError, TopicPartition,
    admin::{
        BatchResult, ListShareGroupOffsetsResult as PublicResult, ShareGroupOffset as PublicOffset,
    },
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, BrokerError, DeliveryStatus,
        Failure, FailureKind, ObserverError, Outcome, PartitionDescription, PartitionError,
        PartitionResult,
    },
    operation::AdminListShareGroupOffsetsResult,
};

pub(super) fn translate_admission_error(error: AdmissionError) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(kind: AdmissionErrorKind) -> KafkaError {
    let public = match kind {
        AdmissionErrorKind::InvalidRequest | AdmissionErrorKind::InvalidDeadline => {
            ErrorKind::Configuration
        }
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
        format!("ListShareGroupOffsets admission failed: {kind:?}"),
    )
    .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "ListShareGroupOffsets was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "ListShareGroupOffsets was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminListShareGroupOffsetsResult {
    match result {
        Ok(Outcome::Offsets(batch)) => Ok(translate_offsets_batch(batch)),
        Ok(Outcome::BrokerRejected(error)) => Err(translate_broker_error(error)),
        Ok(Outcome::Batch(_)) => Err(KafkaError::new(
            ErrorKind::Internal,
            "singular ListShareGroupOffsets received a batch terminal",
        )
        .with_delivery_status(PublicDeliveryStatus::PossiblySent)),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

pub(super) fn translate_offsets_batch(batch: super::engine::OffsetsBatch) -> PublicResult {
    let (throttle_time_ms, partitions) = batch.into_parts();
    let entries = partitions
        .into_iter()
        .map(translate_partition_result)
        .collect();
    PublicResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        BatchResult::new(entries),
    )
}

pub(super) fn translate_partition_result(
    partition: PartitionResult,
) -> (TopicPartition, Result<PublicOffset, KafkaError>) {
    let (topic, topic_id, partition, result) = partition.into_parts();
    (
        TopicPartition::new(topic, partition),
        result
            .map(|description| translate_partition_description(topic_id, description))
            .map_err(translate_partition_error),
    )
}

fn translate_partition_description(
    topic_id: [u8; 16],
    description: PartitionDescription,
) -> PublicOffset {
    let (start_offset, leader_epoch, lag) = description.into_parts();
    PublicOffset::new(topic_id, start_offset, leader_epoch, lag)
}

fn translate_partition_error(error: PartitionError) -> KafkaError {
    let (code, message, message_truncated) = error.into_parts();
    translate_partition_error_parts(code, message.as_deref(), message_truncated)
}

pub(super) fn translate_partition_error_parts(
    code: i16,
    message: Option<&str>,
    message_truncated: bool,
) -> KafkaError {
    broker_error(
        "Kafka rejected ListShareGroupOffsets partition",
        code,
        message,
        message_truncated,
    )
}

pub(super) fn translate_broker_error(error: BrokerError) -> KafkaError {
    let (throttle_time_ms, code, message, message_truncated) = error.into_parts();
    let context =
        format!("Kafka rejected ListShareGroupOffsets after {throttle_time_ms} ms throttle");
    broker_error(&context, code, message.as_deref(), message_truncated)
}

fn broker_error(
    context: &str,
    code: i16,
    message: Option<&str>,
    message_truncated: bool,
) -> KafkaError {
    let diagnostic = match message {
        Some(message) => format!("{context} with broker code {code}: {message}"),
        None => format!("{context} with broker code {code}"),
    };
    KafkaError::new(ErrorKind::Broker, diagnostic)
        .with_broker_code(Some(code))
        .with_delivery_status(PublicDeliveryStatus::PossiblySent)
        .with_diagnostic_truncated(message_truncated)
}

pub(super) fn translate_failure(failure: Failure) -> KafkaError {
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
    KafkaError::new(public, format!("ListShareGroupOffsets failed: {kind:?}"))
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
