//! Concrete bounded `CreateTopics` ownership without a generic admin framework.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
pub(crate) mod retention;
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
pub(crate) use host::{
    CREATE_TOPICS_CAPACITY, CreateTopicsHost, CreateTopicsHostError, CreateTopicsTurn,
};
pub use model::{CreateTopic, CreateTopicConfig, CreateTopicsRequest};
pub use observer::CreateTopicsObserver;
pub use outcome::{
    CreateTopicError, CreateTopicResult, CreateTopicsDeliveryStatus, CreateTopicsFailure,
    CreateTopicsFailureKind, CreateTopicsObserverError, CreateTopicsOutcome,
};
pub(crate) use shard::{
    CreateTopicsAdmissionPort, CreateTopicsShardLockError, CreateTopicsShardOwner,
    CreateTopicsShardWake, CreateTopicsShardWakeError,
};
