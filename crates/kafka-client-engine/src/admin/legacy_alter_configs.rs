//! Declarative facade for bounded resource-generic `LegacyAlterConfigs` ownership.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub(crate) use error::LegacyAlterConfigsHostError;
pub use error::{LegacyAlterConfigsAdmissionError, LegacyAlterConfigsAdmissionErrorKind};
pub use handle::{
    LegacyAlterConfigsAccepted, LegacyAlterConfigsAcceptedFaultKind, LegacyAlterConfigsCapture,
};
pub(crate) use host::{
    LEGACY_ALTER_CONFIGS_CAPACITY, LegacyAlterConfigsHost, LegacyAlterConfigsTurn,
};
pub use model::{
    LegacyAlterConfigsRequest, LegacyConfigEntry, LegacyConfigResourceReplacement,
    LegacyTopicConfigReplacement,
};
pub use observer::LegacyAlterConfigsObserver;
pub use outcome::{
    LegacyAlterConfigError, LegacyAlterConfigResult, LegacyAlterConfigsDeliveryStatus,
    LegacyAlterConfigsFailure, LegacyAlterConfigsFailureKind, LegacyAlterConfigsObserverError,
    LegacyAlterConfigsOutcome, LegacyAlterConfigsResult,
};
pub(crate) use shard::{
    LegacyAlterConfigsAdmissionPort, LegacyAlterConfigsShardLockError,
    LegacyAlterConfigsShardOwner, LegacyAlterConfigsShardWake, LegacyAlterConfigsShardWakeError,
};
#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod observer_test;
#[cfg(test)]
mod outcome_test;
