//! Declarative facade for concrete topic `DescribeConfigs` execution.

mod error;
mod handle;
#[cfg(test)]
mod handle_test;
mod host;
#[cfg(test)]
mod host_routing_test;
#[cfg(test)]
mod host_test;
mod model;
#[cfg(test)]
mod model_test;
mod observer;
mod observer_error;
mod outcome;
#[cfg(test)]
mod outcome_test;
mod shard;
#[cfg(test)]
mod shard_test;
mod translate;

pub use error::{DescribeConfigsAdmissionError, DescribeConfigsAdmissionErrorKind};
pub use handle::{DescribeConfigsAccepted, DescribeConfigsAcceptedFaultKind};
pub(crate) use host::{
    DESCRIBE_CONFIGS_CAPACITY, DescribeConfigsHost, DescribeConfigsHostError, DescribeConfigsTurn,
};
pub(crate) use model::DescribeConfigsRetention;
pub use model::{DescribeConfigsRequest, DescribeConfigsResourceQuery};
pub use observer::DescribeConfigsObserver;
pub use observer_error::DescribeConfigsObserverError;
pub use outcome::{
    DescribeConfigEntry, DescribeConfigResourceError, DescribeConfigResourceResult,
    DescribeConfigSynonym, DescribeConfigsBatch, DescribeConfigsDeliveryStatus,
    DescribeConfigsFailure, DescribeConfigsFailureKind, DescribeConfigsOutcome,
};
pub(crate) use shard::{
    DescribeConfigsAdmissionPort, DescribeConfigsShardLockError, DescribeConfigsShardOwner,
    DescribeConfigsShardWake, DescribeConfigsShardWakeError,
};
