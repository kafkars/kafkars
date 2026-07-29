//! Curated API 33 legacy replacement exports split from the main admin facade.

pub use super::legacy_alter_configs::{
    LegacyAlterConfigError, LegacyAlterConfigResult, LegacyAlterConfigsAccepted,
    LegacyAlterConfigsAcceptedFaultKind, LegacyAlterConfigsAdmissionError,
    LegacyAlterConfigsAdmissionErrorKind, LegacyAlterConfigsCapture,
    LegacyAlterConfigsDeliveryStatus, LegacyAlterConfigsFailure, LegacyAlterConfigsFailureKind,
    LegacyAlterConfigsObserver, LegacyAlterConfigsObserverError, LegacyAlterConfigsOutcome,
    LegacyAlterConfigsRequest, LegacyAlterConfigsResult, LegacyConfigEntry,
    LegacyConfigResourceReplacement, LegacyTopicConfigReplacement,
};
pub(crate) use super::legacy_alter_configs::{
    LegacyAlterConfigsAdmissionPort, LegacyAlterConfigsHost, LegacyAlterConfigsHostError,
    LegacyAlterConfigsShardOwner,
};
