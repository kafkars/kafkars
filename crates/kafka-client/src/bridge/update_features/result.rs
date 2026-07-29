//! Exhaustive stable translation of engine-owned UpdateFeatures outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{BatchResult, UpdateFeaturesResult},
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, Batch, BrokerError, DeliveryStatus,
        Failure, FailureKind, FeatureOutcome, FeatureResult, ObserverError, Outcome,
    },
    operation::AdminUpdateFeaturesResult,
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
    KafkaError::new(public, format!("UpdateFeatures admission failed: {kind:?}"))
        .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "UpdateFeatures was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "UpdateFeatures was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminUpdateFeaturesResult {
    match result {
        Ok(Outcome::Updated(batch)) => Ok(translate_batch(batch)),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_batch(batch: Batch) -> UpdateFeaturesResult {
    let (throttle_time_ms, outcomes) = batch.into_parts();
    translate_batch_parts(
        throttle_time_ms,
        outcomes
            .into_iter()
            .map(translate_feature_outcome)
            .collect(),
    )
}

pub(super) fn translate_batch_parts(
    throttle_time_ms: u32,
    outcomes: Vec<(String, Result<(), KafkaError>)>,
) -> UpdateFeaturesResult {
    UpdateFeaturesResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        BatchResult::new(outcomes),
    )
}

fn translate_feature_outcome(outcome: FeatureOutcome) -> (String, Result<(), KafkaError>) {
    let (feature_name, result) = outcome.into_parts();
    match result {
        FeatureResult::Updated => translate_feature_parts(feature_name, None),
        FeatureResult::Failed(error) => {
            let (code, message, message_truncated) = error.into_parts();
            translate_feature_parts(
                feature_name,
                Some((code, message.as_deref(), message_truncated)),
            )
        }
    }
}

pub(super) fn translate_feature_parts(
    feature_name: String,
    error: Option<(i16, Option<&str>, bool)>,
) -> (String, Result<(), KafkaError>) {
    let result = error.map_or(Ok(()), |(code, message, message_truncated)| {
        Err(translate_broker_parts(
            "feature",
            code,
            message,
            message_truncated,
            PublicDeliveryStatus::PossiblySent,
        ))
    });
    (feature_name, result)
}

fn translate_failure(failure: Failure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(kind: &FailureKind, delivery: DeliveryStatus) -> KafkaError {
    match kind {
        FailureKind::Broker(error) => translate_whole_broker_error(error, delivery),
        FailureKind::DeadlineElapsed => {
            translate_mechanism_failure(kind, ErrorKind::Timeout, delivery)
        }
        FailureKind::DriverRejected | FailureKind::ResponseTooLarge => {
            translate_mechanism_failure(kind, ErrorKind::Backpressure, delivery)
        }
        FailureKind::Transport => translate_mechanism_failure(kind, ErrorKind::Transport, delivery),
        FailureKind::Compatibility => {
            translate_mechanism_failure(kind, ErrorKind::Compatibility, delivery)
        }
        FailureKind::InvalidResponse => {
            translate_mechanism_failure(kind, ErrorKind::Broker, delivery)
        }
    }
}

fn translate_mechanism_failure(
    kind: &FailureKind,
    public: ErrorKind,
    delivery: DeliveryStatus,
) -> KafkaError {
    KafkaError::new(public, format!("UpdateFeatures failed: {kind:?}"))
        .with_delivery_status(translate_delivery(delivery))
}

fn translate_whole_broker_error(error: &BrokerError, delivery: DeliveryStatus) -> KafkaError {
    translate_broker_parts(
        "operation",
        error.code(),
        error.message(),
        error.message_truncated(),
        translate_delivery(delivery),
    )
}

pub(super) fn translate_broker_parts(
    scope: &str,
    code: i16,
    message: Option<&str>,
    message_truncated: bool,
    delivery: PublicDeliveryStatus,
) -> KafkaError {
    let diagnostic = match message {
        Some(message) => {
            format!("Kafka rejected UpdateFeatures {scope} with broker code {code}: {message}")
        }
        None => format!("Kafka rejected UpdateFeatures {scope} with broker code {code}"),
    };
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
