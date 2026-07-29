//! Exhaustive core-to-engine feature terminal translation.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, DescribeFeaturesDescription as CoreDescription,
    DescribeFeaturesFailureKind as CoreFailureKind,
    DescribeFeaturesFinalizedFeature as CoreFinalizedFeature,
    DescribeFeaturesSupportedFeature as CoreSupportedFeature,
    DescribeFeaturesTerminal as CoreTerminal,
};

use super::super::{
    DescribeFeaturesDescription, DescribeFeaturesFinalizedFeature, DescribeFeaturesSupportedFeature,
};
use super::{
    DescribeFeaturesBrokerError, DescribeFeaturesDeliveryStatus, DescribeFeaturesFailure,
    DescribeFeaturesFailureKind, DescribeFeaturesOutcome,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> DescribeFeaturesOutcome {
    match terminal {
        CoreTerminal::Described(description) => {
            DescribeFeaturesOutcome::Described(translate_description(description))
        }
        CoreTerminal::BrokerRejected(error) => {
            let (throttle_time_ms, code) = error.into_parts();
            DescribeFeaturesOutcome::BrokerRejected(DescribeFeaturesBrokerError {
                throttle_time_ms,
                code,
            })
        }
        CoreTerminal::Failed(failure) => DescribeFeaturesOutcome::Failed(DescribeFeaturesFailure {
            kind: failure_kind(failure.kind()),
            delivery: delivery(failure.delivery()),
        }),
    }
}

fn translate_description(description: CoreDescription) -> DescribeFeaturesDescription {
    let (
        throttle_time_ms,
        supported_features,
        supported_features_complete,
        finalized_features_epoch,
        finalized_features,
        zk_migration_ready,
    ) = description.into_parts();
    DescribeFeaturesDescription {
        throttle_time_ms,
        supported_features: supported_features
            .into_iter()
            .map(translate_supported)
            .collect(),
        supported_features_complete,
        finalized_features_epoch,
        finalized_features: finalized_features
            .into_iter()
            .map(translate_finalized)
            .collect(),
        zk_migration_ready,
    }
}

fn translate_supported(feature: CoreSupportedFeature) -> DescribeFeaturesSupportedFeature {
    let (name, min_version, max_version) = feature.into_parts();
    DescribeFeaturesSupportedFeature {
        name,
        min_version,
        max_version,
    }
}

fn translate_finalized(feature: CoreFinalizedFeature) -> DescribeFeaturesFinalizedFeature {
    let (name, min_version_level, max_version_level) = feature.into_parts();
    DescribeFeaturesFinalizedFeature {
        name,
        min_version_level,
        max_version_level,
    }
}

const fn failure_kind(kind: CoreFailureKind) -> DescribeFeaturesFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => DescribeFeaturesFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => DescribeFeaturesFailureKind::DriverRejected,
        CoreFailureKind::Transport => DescribeFeaturesFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => DescribeFeaturesFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => DescribeFeaturesFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => DescribeFeaturesFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> DescribeFeaturesDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => DescribeFeaturesDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => DescribeFeaturesDeliveryStatus::PossiblySent,
    }
}
