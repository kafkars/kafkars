//! Declarative ownership boundary for the first synchronized assigned consumer.

mod claim;
#[cfg(test)]
mod claim_test;
mod event_port;
#[cfg(test)]
mod event_port_test;
mod handle;
#[cfg(test)]
mod handle_test;
mod port;
#[cfg(test)]
mod port_test;
mod reclaim;
#[cfg(test)]
mod reclaim_test;
mod result;
#[cfg(test)]
mod result_test;
mod shard;
#[cfg(test)]
mod shard_test;
mod start;
#[cfg(test)]
mod start_test;
mod state;
#[cfg(test)]
mod state_test;
mod wake;
#[cfg(test)]
mod wake_test;

pub use claim::AssignedConsumerClaimError;
pub(crate) use claim::{AssignedConsumerAdmissionCloser, AssignedConsumerClaimSlot};
pub use handle::AssignedConsumerHandle;
pub(crate) use shard::{
    AssignedConsumerPort, AssignedConsumerShardLockError, AssignedConsumerShardOwner,
    AssignedConsumerShutdownStart,
};
pub(crate) use start::build_first_assigned_consumer;
pub(crate) use wake::{AssignedConsumerShardWake, AssignedConsumerShardWakeError};
