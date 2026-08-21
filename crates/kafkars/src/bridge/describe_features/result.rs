//! Exhaustive stable translation of engine Kafka feature discovery outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{
        DescribeFeaturesResult as PublicResult, FinalizedFeature as PublicFinalizedFeature,
        SupportedFeature as PublicSupportedFeature,
    },
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, BrokerError, DeliveryStatus,
        Description, Failure, FailureKind, FinalizedFeature, ObserverError, Outcome,
        SupportedFeature,
    },
    operation::AdminDescribeFeaturesResult,
};

pub(super) fn translate_admission_error(error: AdmissionError) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(kind: AdmissionErrorKind) -> KafkaError {
    let public = match kind {
        AdmissionErrorKind::InvalidDeadline => ErrorKind::Configuration,
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
        format!("DescribeFeatures admission failed: {kind:?}"),
    )
    .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DescribeFeatures was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DescribeFeatures was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminDescribeFeaturesResult {
    match result {
        Ok(Outcome::Described(description)) => Ok(translate_description(description)),
        Ok(Outcome::BrokerRejected(error)) => Err(translate_broker_error(error)),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_description(description: Description) -> PublicResult {
    let (
        throttle_time_ms,
        supported_features,
        supported_features_complete,
        finalized_features_epoch,
        finalized_features,
        zk_migration_ready,
    ) = description.into_parts();
    translate_description_parts(
        throttle_time_ms,
        supported_features
            .into_iter()
            .map(translate_supported_feature)
            .collect(),
        supported_features_complete,
        finalized_features_epoch,
        finalized_features
            .into_iter()
            .map(translate_finalized_feature)
            .collect(),
        zk_migration_ready,
    )
}

pub(super) fn translate_description_parts(
    throttle_time_ms: u32,
    supported_features: Vec<PublicSupportedFeature>,
    supported_features_complete: bool,
    finalized_features_epoch: Option<i64>,
    finalized_features: Vec<PublicFinalizedFeature>,
    zk_migration_ready: bool,
) -> PublicResult {
    PublicResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        supported_features,
        supported_features_complete,
        finalized_features_epoch,
        finalized_features,
        zk_migration_ready,
    )
}

fn translate_supported_feature(feature: SupportedFeature) -> PublicSupportedFeature {
    let (name, min_version_level, max_version_level) = feature.into_parts();
    PublicSupportedFeature::new(name, min_version_level, max_version_level)
}

fn translate_finalized_feature(feature: FinalizedFeature) -> PublicFinalizedFeature {
    let (name, min_version_level, max_version_level) = feature.into_parts();
    PublicFinalizedFeature::new(name, min_version_level, max_version_level)
}

fn translate_broker_error(error: BrokerError) -> KafkaError {
    let (throttle_time_ms, code) = error.into_parts();
    translate_broker_error_parts(throttle_time_ms, code)
}

pub(super) fn translate_broker_error_parts(throttle_time_ms: u32, code: i16) -> KafkaError {
    KafkaError::new(
        ErrorKind::Broker,
        format!(
            "Kafka rejected DescribeFeatures with broker code {code} after \
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
    let public = match kind {
        FailureKind::DeadlineElapsed => ErrorKind::Timeout,
        FailureKind::DriverRejected | FailureKind::ResponseTooLarge => ErrorKind::Backpressure,
        FailureKind::Transport => ErrorKind::Transport,
        FailureKind::Compatibility => ErrorKind::Compatibility,
        FailureKind::InvalidResponse => ErrorKind::Broker,
    };
    KafkaError::new(public, format!("DescribeFeatures failed: {kind:?}"))
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
