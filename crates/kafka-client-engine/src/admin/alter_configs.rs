//! Declarative facade for bounded topic `IncrementalAlterConfigs` ownership.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub(crate) use error::IncrementalAlterConfigsHostError;
pub use error::{IncrementalAlterConfigsAdmissionError, IncrementalAlterConfigsAdmissionErrorKind};
pub use handle::{IncrementalAlterConfigsAccepted, IncrementalAlterConfigsAcceptedFaultKind};
pub(crate) use host::{
    INCREMENTAL_ALTER_CONFIGS_CAPACITY, IncrementalAlterConfigsHost, IncrementalAlterConfigsTurn,
};
pub use model::{
    IncrementalAlterConfigsRequest, IncrementalConfigAlteration, IncrementalConfigOperation,
    TopicConfigAlterations,
};
pub use observer::IncrementalAlterConfigsObserver;
pub use outcome::{
    IncrementalAlterConfigError, IncrementalAlterConfigResult,
    IncrementalAlterConfigsDeliveryStatus, IncrementalAlterConfigsFailure,
    IncrementalAlterConfigsFailureKind, IncrementalAlterConfigsObserverError,
    IncrementalAlterConfigsOutcome, IncrementalAlterConfigsResult,
};
pub(crate) use shard::{
    IncrementalAlterConfigsAdmissionPort, IncrementalAlterConfigsShardLockError,
    IncrementalAlterConfigsShardOwner, IncrementalAlterConfigsShardWake,
    IncrementalAlterConfigsShardWakeError,
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
#[cfg(test)]
mod shard_test;
