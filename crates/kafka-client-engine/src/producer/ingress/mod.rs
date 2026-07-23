//! Thread-safe producer-shard admission without runtime or driver coupling.

mod data;
mod port;
mod shard;
mod terminal;

#[cfg(test)]
pub(crate) use data::{ProducerShardData, ProducerShardStats};
#[cfg_attr(
    not(test),
    expect(unused_imports, reason = "integrated engine host bridge follows")
)]
pub(crate) use port::{
    ProducerAdmissionPort, ProducerPortAccepted, ProducerPortAcceptedFault,
    ProducerPortAdmissionError, ProducerPortPoison, ProducerPortPoisonReason, ProducerPortRejected,
    ProducerPortRejectionReason,
};
pub(crate) use shard::{
    ProducerShardLockError, ProducerShardOwner, ProducerShardWake, ProducerShardWakeError,
};
pub(crate) use terminal::ProducerShardTerminalError;

#[cfg(test)]
mod data_test;
#[cfg(test)]
mod port_test;
#[cfg(test)]
mod shard_test;
#[cfg(test)]
mod terminal_test;
#[cfg(test)]
pub(crate) use shard_test::CountingWake;
