//! Thread-safe producer-shard admission without runtime or driver coupling.

mod data;
mod data_terminal;
mod flush_outcome;
mod outcome;
mod port;
mod reactor_wake;
mod shard;
mod terminal;

#[cfg(test)]
pub(crate) use data::{ProducerShardData, ProducerShardStats};
pub(crate) use flush_outcome::{ProducerPortFlushAccepted, ProducerPortFlushError};
#[cfg_attr(
    not(test),
    expect(unused_imports, reason = "integrated engine host bridge follows")
)]
pub(crate) use outcome::{
    ProducerPortAccepted, ProducerPortAcceptedFault, ProducerPortAdmissionError,
    ProducerPortPoison, ProducerPortPoisonReason, ProducerPortRejected,
    ProducerPortRejectionReason,
};
pub(crate) use port::ProducerAdmissionPort;
pub(crate) use shard::{
    ProducerShardLockError, ProducerShardOwner, ProducerShardWake, ProducerShardWakeError,
};
pub(crate) use terminal::ProducerShardTerminalError;

#[cfg(test)]
mod close_test;
#[cfg(test)]
mod data_terminal_test;
#[cfg(test)]
mod data_test;
#[cfg(test)]
mod flush_outcome_test;
#[cfg(test)]
mod port_test;
#[cfg(test)]
mod reactor_wake_test;
#[cfg(test)]
mod shard_test;
#[cfg(test)]
mod terminal_test;
#[cfg(test)]
pub(crate) use shard_test::CountingWake;
