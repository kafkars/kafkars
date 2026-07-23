//! Unique ownership of the embedded `kafka-driver` reactor and its controls.

mod endpoint;
mod error;
pub(crate) mod owner;
#[cfg(test)]
mod owner_test;
mod wake;
#[cfg(test)]
mod wake_test;

pub(crate) use endpoint::EndpointError;
pub(crate) use error::DriverOwnerError;
pub(crate) use owner::{DriverOwner, DriverTurn};
pub(crate) use wake::{ReactorWake, ReactorWakeError};
