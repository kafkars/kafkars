//! Exhaustive stable translation of engine-owned SCRAM alteration outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{AlterUserScramCredentialsResult, BatchResult},
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, Batch, BrokerError, DeliveryStatus,
        Failure, FailureKind, ObserverError, Outcome, UserOutcome,
    },
    operation::AdminAlterUserScramCredentialsResult,
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
        AdmissionErrorKind::Preparation
        | AdmissionErrorKind::HostInvariant
        | AdmissionErrorKind::IdentityExhausted
        | AdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    KafkaError::new(
        public,
        format!("AlterUserScramCredentials admission failed: {kind:?}"),
    )
    .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "AlterUserScramCredentials was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "AlterUserScramCredentials was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminAlterUserScramCredentialsResult {
    match result {
        Ok(Outcome::Altered(batch)) => Ok(translate_batch(batch)),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(&failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_batch(batch: Batch) -> AlterUserScramCredentialsResult {
    let (throttle_time_ms, outcomes) = batch.into_parts();
    AlterUserScramCredentialsResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        BatchResult::new(outcomes.into_iter().map(translate_user_outcome).collect()),
    )
}

fn translate_user_outcome(outcome: UserOutcome) -> (String, Result<(), KafkaError>) {
    let (user, result) = outcome.into_parts();
    (user, result.map_err(translate_broker_error))
}

fn translate_broker_error(error: BrokerError) -> KafkaError {
    let (code, message, message_truncated) = error.into_parts();
    translate_broker_parts(code, message.as_deref(), message_truncated)
}

pub(super) fn translate_broker_parts(
    code: i16,
    message: Option<&str>,
    message_truncated: bool,
) -> KafkaError {
    let diagnostic = match message {
        Some(message) => {
            format!(
                "Kafka rejected SCRAM credential alteration for user with code {code}: {message}"
            )
        }
        None => format!("Kafka rejected SCRAM credential alteration for user with code {code}"),
    };
    KafkaError::new(ErrorKind::Broker, diagnostic)
        .with_broker_code(Some(code))
        .with_delivery_status(PublicDeliveryStatus::PossiblySent)
        .with_diagnostic_truncated(message_truncated)
}

fn translate_failure(failure: &Failure) -> KafkaError {
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
    KafkaError::new(
        public,
        format!("AlterUserScramCredentials failed: {kind:?}"),
    )
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
