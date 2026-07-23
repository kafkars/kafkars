//! Concrete bounded `CreateTopics` ownership without a generic admin framework.

mod error;
#[allow(dead_code, reason = "the engine constructs the public handle next")]
mod handle;
#[allow(
    dead_code,
    reason = "engine-host turns consume these bounded admin mechanisms next"
)]
mod host;
mod model;
mod observer;
mod outcome;
pub(crate) mod retention;
#[allow(dead_code, reason = "engine-host turns consume this admin shard next")]
mod shard;

#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod observer_test;
#[cfg(test)]
mod retention_test;
#[cfg(test)]
mod shard_test;

pub use error::{CreateTopicsAdmissionError, CreateTopicsAdmissionErrorKind};
pub use handle::{AdminHandle, CreateTopicsAccepted, CreateTopicsAcceptedFaultKind};
#[allow(
    unused_imports,
    reason = "engine-host turns consume these bounded admin mechanisms next"
)]
pub(crate) use host::{
    CREATE_TOPICS_CAPACITY, CreateTopicsHost, CreateTopicsHostError, CreateTopicsTurn,
};
pub use model::{CreateTopic, CreateTopicConfig, CreateTopicsRequest};
pub use observer::CreateTopicsObserver;
pub use outcome::{
    CreateTopicError, CreateTopicResult, CreateTopicsDeliveryStatus, CreateTopicsFailure,
    CreateTopicsFailureKind, CreateTopicsObserverError, CreateTopicsOutcome,
};
#[allow(
    unused_imports,
    reason = "engine-host turns consume this admin shard next"
)]
pub(crate) use shard::{
    CreateTopicsAdmissionPort, CreateTopicsShardLockError, CreateTopicsShardOwner,
    CreateTopicsShardWake, CreateTopicsShardWakeError,
};
