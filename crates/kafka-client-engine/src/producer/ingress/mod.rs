//! Thread-safe producer-shard admission without runtime or driver coupling.

mod data;
mod data_terminal;
mod outcome;
mod pending_fatal;
mod pending_local_fatal;
mod pending_local_settlement;
mod pending_settlement;
mod port;
mod promotion;
mod promotion_error;
mod promotion_rejection;
mod reactor_wake;
mod shard;
mod shard_turn;
mod shard_turn_failure;
mod shard_turn_progress;
mod terminal;
mod waiting;

#[cfg(test)]
pub(crate) use data::{ProducerShardData, ProducerShardStats};
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
pub(crate) use waiting::ProducerWaitingStart;

#[cfg(test)]
mod data_terminal_test;
#[cfg(test)]
mod data_test;
#[cfg(test)]
mod pending_fatal_test;
#[cfg(test)]
mod pending_local_fatal_test;
#[cfg(test)]
mod pending_local_settlement_test;
#[cfg(test)]
mod pending_settlement_test;
#[cfg(test)]
mod port_test;
#[cfg(test)]
mod promotion_error_test;
#[cfg(test)]
mod promotion_rejection_test;
#[cfg(test)]
mod promotion_test;
#[cfg(test)]
mod reactor_wake_test;
#[cfg(test)]
mod shard_test;
#[cfg(test)]
mod shard_turn_failure_test;
#[cfg(test)]
mod shard_turn_progress_test;
#[cfg(test)]
mod shard_turn_test;
#[cfg(test)]
mod terminal_test;
#[cfg(test)]
mod waiting_classification_test;
#[cfg(test)]
mod waiting_test;
#[cfg(test)]
pub(crate) use shard_test::CountingWake;
