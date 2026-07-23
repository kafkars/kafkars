//! Concrete bounded `CreateTopics` ownership without a generic admin framework.

#[allow(
    dead_code,
    reason = "admission consumes these values in the next ownership slice"
)]
mod error;
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
mod model_test;
#[cfg(test)]
mod observer_test;
#[cfg(test)]
mod retention_test;

pub use error::{CreateTopicsAdmissionError, CreateTopicsAdmissionErrorKind};
pub use model::{CreateTopic, CreateTopicConfig, CreateTopicsRequest};
pub use observer::CreateTopicsObserver;
pub use outcome::{
    CreateTopicError, CreateTopicResult, CreateTopicsDeliveryStatus, CreateTopicsFailure,
    CreateTopicsFailureKind, CreateTopicsObserverError, CreateTopicsOutcome,
};
