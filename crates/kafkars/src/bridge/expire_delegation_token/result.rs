//! Exhaustive stable translation of delegation-token expiration outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::ExpireDelegationTokenResult as PublicResult,
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, BrokerError, DeliveryStatus,
        Failure, FailureKind, ObserverError, Outcome, Result as EngineResult,
    },
    operation::AdminExpireDelegationTokenResult,
};

pub(super) fn translate_admission_error(error: AdmissionError) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(kind: AdmissionErrorKind) -> KafkaError {
    let public = match kind {
        AdmissionErrorKind::InvalidRequest | AdmissionErrorKind::InvalidDeadline => {
            ErrorKind::Configuration
        }
        AdmissionErrorKind::DeadlineElapsed => ErrorKind::Timeout,
        AdmissionErrorKind::Contended
        | AdmissionErrorKind::Capacity
        | AdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        AdmissionErrorKind::Closed => ErrorKind::State,
        AdmissionErrorKind::HostInvariant
        | AdmissionErrorKind::IdentityExhausted
        | AdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    KafkaError::new(
        public,
        format!("ExpireDelegationToken admission failed: {kind:?}"),
    )
    .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "ExpireDelegationToken was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "ExpireDelegationToken was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminExpireDelegationTokenResult {
    match result {
        Ok(Outcome::Expired(result)) => Ok(translate_success(result)),
        Ok(Outcome::BrokerRejected(error)) => Err(translate_broker_error(error)),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_success(result: EngineResult) -> PublicResult {
    let (throttle_time_ms, expiry_timestamp_ms) = result.into_parts();
    PublicResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        expiry_timestamp_ms,
    )
}

fn translate_broker_error(error: BrokerError) -> KafkaError {
    let (throttle_time_ms, code) = error.into_parts();
    translate_broker_error_parts(throttle_time_ms, code)
}

pub(super) fn translate_broker_error_parts(throttle_time_ms: u32, code: i16) -> KafkaError {
    KafkaError::new(
        ErrorKind::Broker,
        format!(
            "Kafka rejected ExpireDelegationToken with broker code {code} after \
             {throttle_time_ms} ms throttle"
        ),
    )
    .with_broker_code(Some(code))
    .with_delivery_status(PublicDeliveryStatus::PossiblySent)
}

fn translate_failure(failure: Failure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(kind: FailureKind, delivery: DeliveryStatus) -> KafkaError {
    KafkaError::new(
        kind_to_error(kind),
        format!("ExpireDelegationToken failed: {kind:?}"),
    )
    .with_delivery_status(translate_delivery(delivery))
}

const fn kind_to_error(kind: FailureKind) -> ErrorKind {
    match kind {
        FailureKind::DeadlineElapsed => ErrorKind::Timeout,
        FailureKind::DriverRejected | FailureKind::ResponseTooLarge => ErrorKind::Backpressure,
        FailureKind::Transport => ErrorKind::Transport,
        FailureKind::Compatibility => ErrorKind::Compatibility,
        FailureKind::InvalidResponse => ErrorKind::Broker,
    }
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
