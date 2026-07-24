//! Declarative facade for bounded topic `IncrementalAlterConfigs` ownership.

mod error;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub(crate) use error::{
    IncrementalAlterConfigsAdmissionErrorKind, IncrementalAlterConfigsHostError,
};
#[cfg(test)]
pub(crate) use host::IncrementalAlterConfigsTurn;
pub(crate) use host::{INCREMENTAL_ALTER_CONFIGS_CAPACITY, IncrementalAlterConfigsHost};
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
