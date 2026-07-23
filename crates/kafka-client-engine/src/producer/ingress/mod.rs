//! Thread-safe producer-shard admission without runtime or driver coupling.

mod port;
mod shard;

#[cfg_attr(
    not(test),
    expect(unused_imports, reason = "integrated engine host bridge follows")
)]
pub(crate) use port::{
    ProducerAdmissionPort, ProducerPortAccepted, ProducerPortAcceptedFault,
    ProducerPortAdmissionError, ProducerPortPoison, ProducerPortPoisonReason, ProducerPortRejected,
    ProducerPortRejectionReason,
};
#[cfg_attr(
    not(test),
    expect(unused_imports, reason = "integrated engine host bridge follows")
)]
pub(crate) use shard::{
    ProducerShardLockError, ProducerShardOwner, ProducerShardWake, ProducerShardWakeError,
};

#[cfg(test)]
mod port_test;
#[cfg(test)]
mod shard_test;
