//! Unique ownership of the embedded `kafka-driver` reactor and its controls.

mod delivery;
#[cfg(test)]
mod delivery_test;
mod endpoint;
mod error;
pub(crate) mod owner;
#[cfg(test)]
mod owner_test;
mod rpc;
mod shutdown;
#[cfg(test)]
mod shutdown_test;
mod wake;
#[cfg(test)]
mod wake_test;

pub(crate) use delivery::request_failure_delivery;
pub(crate) use endpoint::EndpointError;
pub(crate) use error::DriverOwnerError;
pub(crate) use owner::{DriverOwner, DriverTurn};
#[allow(
    unused_imports,
    reason = "engine-host turns consume tracked create-topics calls next"
)]
pub(crate) use rpc::{
    CreateTopicsCompletionFailure, ProduceCompletionFailure, TrackedCreateTopicsCalls,
    TrackedProduceCalls,
};
pub(crate) use wake::{ReactorWake, ReactorWakeError};
