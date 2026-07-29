//! Exhaustive stable translation of engine Admin `DescribeTransactions` outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{BatchResult, DescribeTransactionsResult, TransactionDescription, TransactionTopic},
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, BrokerError, DeliveryStatus,
        Description, Failure, FailureKind, ObserverError, Outcome, Topic,
    },
    operation::AdminDescribeTransactionsResult,
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
        format!("DescribeTransactions admission failed: {kind:?}"),
    )
    .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DescribeTransactions was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DescribeTransactions was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminDescribeTransactionsResult {
    match result {
        Ok(Outcome::Described(batch)) => {
            let (throttle_time_ms, transactions) = batch.into_parts();
            let entries = transactions
                .into_iter()
                .map(|transaction| {
                    let (transactional_id, result) = transaction.into_parts();
                    (
                        transactional_id,
                        result
                            .map(translate_description)
                            .map_err(translate_broker_error),
                    )
                })
                .collect();
            Ok(DescribeTransactionsResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                BatchResult::new(entries),
            ))
        }
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_description(description: Description) -> TransactionDescription {
    let (
        transaction_state,
        transaction_timeout_ms,
        transaction_start_time_ms,
        producer_id,
        producer_epoch,
        topics,
    ) = description.into_parts();
    transaction_description_from_parts(
        transaction_state,
        transaction_timeout_ms,
        transaction_start_time_ms,
        producer_id,
        producer_epoch,
        topics.into_iter().map(translate_topic).collect(),
    )
}

const fn transaction_description_from_parts(
    transaction_state: String,
    transaction_timeout_ms: i32,
    transaction_start_time_ms: Option<i64>,
    producer_id: i64,
    producer_epoch: i16,
    topics: Vec<TransactionTopic>,
) -> TransactionDescription {
    TransactionDescription::new(
        transaction_state,
        transaction_timeout_ms,
        transaction_start_time_ms,
        producer_id,
        producer_epoch,
        topics,
    )
}

fn translate_topic(topic: Topic) -> TransactionTopic {
    let (topic, partitions) = topic.into_parts();
    transaction_topic_from_parts(topic, partitions)
}

const fn transaction_topic_from_parts(topic: String, partitions: Vec<i32>) -> TransactionTopic {
    TransactionTopic::new(topic, partitions)
}

fn translate_broker_error(error: BrokerError) -> KafkaError {
    translate_broker_error_code(error.code())
}

pub(super) fn translate_broker_error_code(code: i16) -> KafkaError {
    KafkaError::new(
        ErrorKind::Broker,
        format!("Kafka returned DescribeTransactions broker code {code}"),
    )
    .with_broker_code(Some(code))
    .with_delivery_status(PublicDeliveryStatus::PossiblySent)
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
    KafkaError::new(public, format!("DescribeTransactions failed: {kind:?}"))
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
