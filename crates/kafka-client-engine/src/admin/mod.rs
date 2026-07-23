//! Concrete bounded `CreateTopics` ownership without a generic admin framework.

#[allow(
    dead_code,
    reason = "admission consumes these values in the next ownership slice"
)]
mod error;
#[allow(
    dead_code,
    reason = "the shard consumes the bounded host in the next ownership slice"
)]
mod host;
#[allow(
    dead_code,
    reason = "admission consumes these values in the next ownership slice"
)]
mod model;
#[allow(
    dead_code,
    reason = "admission consumes these values in the next ownership slice"
)]
mod observer;
mod outcome;
#[allow(
    dead_code,
    reason = "admission consumes this charge in the next ownership slice"
)]
pub(crate) mod retention;

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod observer_test;
#[cfg(test)]
mod retention_test;

pub use error::{CreateTopicsAdmissionError, CreateTopicsAdmissionErrorKind};
#[allow(
    unused_imports,
    reason = "the shard consumes the bounded host in the next ownership slice"
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
