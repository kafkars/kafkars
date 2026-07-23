//! Unique ownership of the embedded `kafka-driver` reactor and its controls.

mod endpoint;
mod error;
pub(crate) mod owner;
#[cfg(test)]
mod owner_test;
mod producer_wake;
#[cfg(test)]
mod producer_wake_test;

pub(crate) use endpoint::EndpointError;
pub(crate) use error::DriverOwnerError;
pub(crate) use producer_wake::ProducerDriverWake;
