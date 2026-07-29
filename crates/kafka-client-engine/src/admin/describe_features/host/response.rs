//! Exhaustive generated-free response and driver-failure translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    DeliveryStatus, DescribeFeaturesBrokerError, DescribeFeaturesDescription,
    DescribeFeaturesFinalizedFeature, DescribeFeaturesInput, DescribeFeaturesSupportedFeature,
};

use crate::{
    driver::{
        DescribeFeaturesDriverFailureKind, DescribeFeaturesRawTerminal,
        DescribeFeaturesTerminalFact,
    },
    protocol::admin::describe_features::{
        DescribeFeaturesProtocolFailure, NormalizedDescribeFeaturesFinalizedFeature,
        NormalizedDescribeFeaturesSupportedFeature, normalize_describe_features_response,
    },
};

pub(super) fn terminal_input(
    raw: &DescribeFeaturesRawTerminal,
    retained_limit: usize,
) -> (DescribeFeaturesInput, usize) {
    match raw.fact() {
        DescribeFeaturesTerminalFact::Response {
            selected_version,
            response,
        } => match normalize_describe_features_response(selected_version, response, retained_limit)
        {
            Ok(normalized) => {
                let (
                    throttle_time_ms,
                    error_code,
                    supported_features,
                    supported_features_complete,
                    finalized_features_epoch,
                    finalized_features,
                    zk_migration_ready,
                    retained_bytes,
                ) = normalized.into_parts();
                (
                    normalized_input(
                        throttle_time_ms,
                        error_code,
                        supported_features,
                        supported_features_complete,
                        finalized_features_epoch,
                        finalized_features,
                        zk_migration_ready,
                    ),
                    retained_bytes,
                )
            }
            Err(error) => (protocol_failure(error), 0),
        },
        DescribeFeaturesTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

pub(super) fn normalized_input(
    throttle_time_ms: u32,
    error_code: i16,
    supported_features: Vec<NormalizedDescribeFeaturesSupportedFeature>,
    supported_features_complete: bool,
    finalized_features_epoch: Option<i64>,
    finalized_features: Vec<NormalizedDescribeFeaturesFinalizedFeature>,
    zk_migration_ready: bool,
) -> DescribeFeaturesInput {
    match NonZeroI16::new(error_code) {
        Some(code)
            if supported_features.is_empty()
                && finalized_features_epoch.is_none()
                && finalized_features.is_empty()
                && !zk_migration_ready =>
        {
            DescribeFeaturesInput::BrokerRejected {
                error: DescribeFeaturesBrokerError::new(throttle_time_ms, code),
            }
        }
        Some(_) => DescribeFeaturesInput::InvalidResponse,
        None => core_description(
            throttle_time_ms,
            supported_features,
            supported_features_complete,
            finalized_features_epoch,
            finalized_features,
            zk_migration_ready,
        )
        .map(|description| DescribeFeaturesInput::BrokerResponded { description })
        .unwrap_or(DescribeFeaturesInput::InvalidResponse),
    }
}

fn core_description(
    throttle_time_ms: u32,
    supported_features: Vec<NormalizedDescribeFeaturesSupportedFeature>,
    supported_features_complete: bool,
    finalized_features_epoch: Option<i64>,
    finalized_features: Vec<NormalizedDescribeFeaturesFinalizedFeature>,
    zk_migration_ready: bool,
) -> Result<DescribeFeaturesDescription, kafka_client_core::DescribeFeaturesValueError> {
    DescribeFeaturesDescription::new(
        throttle_time_ms,
        supported_features.into_iter().map(core_supported).collect(),
        supported_features_complete,
        finalized_features_epoch,
        finalized_features.into_iter().map(core_finalized).collect(),
        zk_migration_ready,
    )
}

fn core_supported(
    feature: NormalizedDescribeFeaturesSupportedFeature,
) -> DescribeFeaturesSupportedFeature {
    let (name, min_version, max_version) = feature.into_parts();
    DescribeFeaturesSupportedFeature::new(name, min_version, max_version)
}

fn core_finalized(
    feature: NormalizedDescribeFeaturesFinalizedFeature,
) -> DescribeFeaturesFinalizedFeature {
    let (name, min_version_level, max_version_level) = feature.into_parts();
    DescribeFeaturesFinalizedFeature::new(name, min_version_level, max_version_level)
}

pub(super) const fn protocol_failure(
    error: DescribeFeaturesProtocolFailure,
) -> DescribeFeaturesInput {
    match error {
        DescribeFeaturesProtocolFailure::MissingSelectedVersion
        | DescribeFeaturesProtocolFailure::UnsupportedApiVersion { .. } => {
            DescribeFeaturesInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        DescribeFeaturesProtocolFailure::RetainedBytes { .. }
        | DescribeFeaturesProtocolFailure::Allocation { .. } => {
            DescribeFeaturesInput::ResponseTooLarge
        }
        DescribeFeaturesProtocolFailure::NegativeThrottleTime { .. }
        | DescribeFeaturesProtocolFailure::TooManyApiKeys { .. }
        | DescribeFeaturesProtocolFailure::InvalidApiKey { .. }
        | DescribeFeaturesProtocolFailure::InvalidApiVersionRange { .. }
        | DescribeFeaturesProtocolFailure::BrokerErrorWithFeaturePayload
        | DescribeFeaturesProtocolFailure::TooManyFeatures { .. }
        | DescribeFeaturesProtocolFailure::EmptyFeatureName { .. }
        | DescribeFeaturesProtocolFailure::FeatureNameTooLong { .. }
        | DescribeFeaturesProtocolFailure::FeatureTextBytesExceeded { .. }
        | DescribeFeaturesProtocolFailure::InvalidFeatureRange { .. }
        | DescribeFeaturesProtocolFailure::InvalidFinalizedFeaturesEpoch { .. }
        | DescribeFeaturesProtocolFailure::FinalizedFeaturesWithoutEpoch
        | DescribeFeaturesProtocolFailure::DuplicateFeatureName { .. } => {
            DescribeFeaturesInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: DescribeFeaturesDriverFailureKind,
    delivery: DeliveryStatus,
) -> DescribeFeaturesInput {
    match kind {
        DescribeFeaturesDriverFailureKind::DeadlineElapsed => {
            DescribeFeaturesInput::DriverDeadlineElapsed { delivery }
        }
        DescribeFeaturesDriverFailureKind::Compatibility => {
            DescribeFeaturesInput::ProtocolIncompatible { delivery }
        }
        DescribeFeaturesDriverFailureKind::InvalidResponse => {
            DescribeFeaturesInput::InvalidResponse
        }
        DescribeFeaturesDriverFailureKind::Transport => {
            DescribeFeaturesInput::TransportFailed { delivery }
        }
    }
}
