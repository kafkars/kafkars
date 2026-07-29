//! Exhaustive stable translation of engine Admin `DescribeProducers` outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError, TopicPartition,
    admin::{BatchResult, DescribeProducersResult, ProducerState},
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, BrokerError, DeliveryStatus,
        Failure, FailureKind, ObserverError, Outcome, ProducerState as EngineProducerState,
    },
    operation::AdminDescribeProducersResult,
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
        format!("DescribeProducers admission failed: {kind:?}"),
    )
    .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DescribeProducers was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DescribeProducers was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminDescribeProducersResult {
    match result {
        Ok(Outcome::Described(batch)) => {
            let (throttle_time_ms, partitions) = batch.into_parts();
            let entries = partitions
                .into_iter()
                .map(|partition| {
                    let (topic, partition, result) = partition.into_parts();
                    (
                        TopicPartition::new(topic, partition),
                        result
                            .map(|states| {
                                states.into_iter().map(translate_producer_state).collect()
                            })
                            .map_err(translate_broker_error),
                    )
                })
                .collect();
            Ok(DescribeProducersResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                BatchResult::new(entries),
            ))
        }
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_producer_state(state: EngineProducerState) -> ProducerState {
    let (
        producer_id,
        producer_epoch,
        last_sequence,
        last_timestamp,
        coordinator_epoch,
        current_transaction_start_offset,
    ) = state.into_parts();
    producer_state_from_parts(
        producer_id,
        producer_epoch,
        last_sequence,
        last_timestamp,
        coordinator_epoch,
        current_transaction_start_offset,
    )
}

pub(super) const fn producer_state_from_parts(
    producer_id: i64,
    producer_epoch: i32,
    last_sequence: i32,
    last_timestamp: i64,
    coordinator_epoch: i32,
    current_transaction_start_offset: Option<i64>,
) -> ProducerState {
    ProducerState::new(
        producer_id,
        producer_epoch,
        last_sequence,
        last_timestamp,
        coordinator_epoch,
        current_transaction_start_offset,
    )
}

fn translate_broker_error(error: BrokerError) -> KafkaError {
    let (code, message, message_truncated) = error.into_parts();
    translate_broker_error_parts(code, message.as_deref(), message_truncated)
}

pub(super) fn translate_broker_error_parts(
    code: i16,
    message: Option<&str>,
    message_truncated: bool,
) -> KafkaError {
    let detail = message.map_or_else(
        || format!("Kafka rejected DescribeProducers partition with broker code {code}"),
        |message| {
            format!("Kafka rejected DescribeProducers partition with broker code {code}: {message}")
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
    KafkaError::new(public, format!("DescribeProducers failed: {kind:?}"))
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
