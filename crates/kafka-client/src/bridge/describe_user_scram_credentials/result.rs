//! Exhaustive stable translation of engine-owned SCRAM description outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{BatchResult, DescribeUserScramCredentialsResult, ScramCredentialInfo, ScramMechanism},
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, Batch, BrokerError, CredentialInfo,
        DeliveryStatus, Failure, FailureKind, ObserverError, Outcome, UserOutcome, UserResult,
    },
    operation::AdminDescribeUserScramCredentialsResult,
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
        format!("DescribeUserScramCredentials admission failed: {kind:?}"),
    )
    .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DescribeUserScramCredentials was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DescribeUserScramCredentials was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminDescribeUserScramCredentialsResult {
    match result {
        Ok(Outcome::Described(batch)) => Ok(translate_batch(batch)),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_batch(batch: Batch) -> DescribeUserScramCredentialsResult {
    let (throttle_time_ms, outcomes) = batch.into_parts();
    DescribeUserScramCredentialsResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        BatchResult::new(outcomes.into_iter().map(translate_user_outcome).collect()),
    )
}

fn translate_user_outcome(
    outcome: UserOutcome,
) -> (String, Result<Vec<ScramCredentialInfo>, KafkaError>) {
    let (user, result) = outcome.into_parts();
    let result = match result {
        UserResult::Described(infos) => Ok(infos.into_iter().map(translate_info).collect()),
        UserResult::BrokerFailed(error) => Err(translate_user_error(error)),
    };
    (user, result)
}

fn translate_info(info: CredentialInfo) -> ScramCredentialInfo {
    let (mechanism, iterations) = info.into_parts();
    translate_info_parts(mechanism, iterations)
}

pub(super) const fn translate_info_parts(mechanism: i8, iterations: u32) -> ScramCredentialInfo {
    ScramCredentialInfo::new(ScramMechanism::from_code(mechanism), iterations)
}

fn translate_user_error(error: BrokerError) -> KafkaError {
    let (code, message, message_truncated) = error.into_parts();
    translate_user_error_parts(code, message.as_deref(), message_truncated)
}

pub(super) fn translate_user_error_parts(
    code: i16,
    message: Option<&str>,
    message_truncated: bool,
) -> KafkaError {
    let diagnostic = match message {
        Some(message) => {
            format!("Kafka rejected SCRAM credential lookup for user with code {code}: {message}")
        }
        None => format!("Kafka rejected SCRAM credential lookup for user with code {code}"),
    };
    KafkaError::new(ErrorKind::Broker, diagnostic)
        .with_broker_code(Some(code))
        .with_delivery_status(PublicDeliveryStatus::PossiblySent)
        .with_diagnostic_truncated(message_truncated)
}

fn translate_failure(failure: Failure) -> KafkaError {
    translate_failure_parts(failure.kind().clone(), failure.delivery())
}

pub(super) fn translate_failure_parts(kind: FailureKind, delivery: DeliveryStatus) -> KafkaError {
    let delivery = translate_delivery(delivery);
    match kind {
        FailureKind::Broker(error) => translate_top_level_broker_error(error, delivery),
        kind => {
            let public = match kind {
                FailureKind::DeadlineElapsed => ErrorKind::Timeout,
                FailureKind::DriverRejected | FailureKind::ResponseTooLarge => {
                    ErrorKind::Backpressure
                }
                FailureKind::Transport => ErrorKind::Transport,
                FailureKind::Compatibility => ErrorKind::Compatibility,
                FailureKind::InvalidResponse => ErrorKind::Broker,
                FailureKind::Broker(_) => unreachable!(),
            };
            KafkaError::new(
                public,
                format!("DescribeUserScramCredentials failed: {kind:?}"),
            )
            .with_delivery_status(delivery)
        }
    }
}

fn translate_top_level_broker_error(
    error: BrokerError,
    delivery: PublicDeliveryStatus,
) -> KafkaError {
    let (code, message, message_truncated) = error.into_parts();
    let diagnostic = message.map_or_else(
        || format!("Kafka rejected DescribeUserScramCredentials with broker code {code}"),
        |message| {
            format!(
                "Kafka rejected DescribeUserScramCredentials with broker code {code}: {message}"
            )
        },
    );
    KafkaError::new(ErrorKind::Broker, diagnostic)
        .with_broker_code(Some(code))
        .with_delivery_status(delivery)
        .with_diagnostic_truncated(message_truncated)
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
