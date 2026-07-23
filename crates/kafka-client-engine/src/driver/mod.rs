//! Unique ownership of the embedded `kafka-driver` reactor and its controls.

#[allow(
    dead_code,
    reason = "terminal translation is consumed by the producer-driver join milestone"
)]
mod delivery;
#[cfg(test)]
mod delivery_test;
mod endpoint;
mod error;
pub(crate) mod owner;
#[cfg(test)]
mod owner_test;
#[allow(
    dead_code,
    reason = "tracked Produce admission is callable before the shard join is linearized"
)]
mod rpc;
#[cfg(test)]
mod rpc_test;
mod shutdown;
#[cfg(test)]
mod shutdown_test;
mod wake;
#[cfg(test)]
mod wake_test;

#[allow(
    unused_imports,
    reason = "reexported for the producer-driver join milestone"
)]
pub(crate) use delivery::request_failure_delivery;
pub(crate) use endpoint::EndpointError;
pub(crate) use error::DriverOwnerError;
pub(crate) use owner::{DriverOwner, DriverTurn};
#[allow(
    unused_imports,
    reason = "reexported for the producer-driver join milestone"
)]
pub(crate) use rpc::ProduceSubmitError;
pub(crate) use wake::{ReactorWake, ReactorWakeError};
