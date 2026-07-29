//! Exhaustive stable translation of producer-fencing outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{BatchResult, FenceProducersResult, FencedProducerIdentity},
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, Batch, BrokerError, DeliveryStatus,
        Failure, FailureKind, Identity, ItemResult, ObserverError, Outcome,
    },
    operation::AdminFenceProducersResult,
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
    KafkaError::new(public, format!("FenceProducers admission failed: {kind:?}"))
        .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "FenceProducers was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "FenceProducers was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
    prepared: Option<PreparedFenceProducerResults>,
) -> AdminFenceProducersResult {
    match result {
        Ok(Outcome::Fenced(batch)) => translate_batch(
            batch,
            prepared.ok_or_else(missing_prepared_result_capacity)?,
        ),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

pub(super) struct PreparedFenceProducerResults {
    expected: usize,
    entries: Vec<(String, Result<FencedProducerIdentity, KafkaError>)>,
}

impl PreparedFenceProducerResults {
    pub(super) fn try_new(expected: usize) -> Result<Self, ()> {
        let mut entries = Vec::new();
        entries.try_reserve_exact(expected).map_err(|_error| ())?;
        Ok(Self { expected, entries })
    }
}

fn translate_batch(
    batch: Batch,
    mut prepared: PreparedFenceProducerResults,
) -> AdminFenceProducersResult {
    let (throttle_time_ms, items) = batch.into_parts();
    if items.len() != prepared.expected
        || prepared
            .entries
            .capacity()
            .saturating_sub(prepared.entries.len())
            < items.len()
    {
        return Err(missing_prepared_result_capacity());
    }
    for item in items {
        prepared.entries.push(translate_item(item));
    }
    Ok(translate_batch_parts(throttle_time_ms, prepared.entries))
}

pub(super) fn translate_batch_parts(
    throttle_time_ms: u32,
    entries: Vec<(String, Result<FencedProducerIdentity, KafkaError>)>,
) -> FenceProducersResult {
    FenceProducersResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        BatchResult::new(entries),
    )
}

fn translate_item(item: ItemResult) -> (String, Result<FencedProducerIdentity, KafkaError>) {
    let (transactional_id, result) = item.into_parts();
    (
        transactional_id,
        result
            .map(translate_identity)
            .map_err(translate_broker_error),
    )
}

fn translate_identity(identity: Identity) -> FencedProducerIdentity {
    let (producer_id, producer_epoch) = identity.into_parts();
    translate_identity_parts(producer_id, producer_epoch)
}

pub(super) const fn translate_identity_parts(
    producer_id: i64,
    producer_epoch: i16,
) -> FencedProducerIdentity {
    FencedProducerIdentity::new(producer_id, producer_epoch)
}

fn translate_broker_error(error: BrokerError) -> KafkaError {
    translate_broker_error_code(error.code())
}

pub(super) fn translate_broker_error_code(code: i16) -> KafkaError {
    KafkaError::new(
        ErrorKind::Broker,
        format!("Kafka rejected producer fencing with broker code {code}"),
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
    KafkaError::new(public, format!("FenceProducers failed: {kind:?}"))
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

fn missing_prepared_result_capacity() -> KafkaError {
    KafkaError::new(
        ErrorKind::Internal,
        "FenceProducers terminal did not match its prepared public result capacity",
    )
    .with_delivery_status(PublicDeliveryStatus::PossiblySent)
}
