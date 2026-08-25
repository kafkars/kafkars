//! Declarative boundary for private transactional lifecycle execution.

mod admission;
mod completion;
mod driver_port;
mod effect;
mod enrollment;
mod host;
mod limits;
mod offset_commit;
mod port;
mod recovery;
mod sequencing;
mod start;
mod turn;

pub(crate) use host::{
    TransactionLifecycleHost, TransactionLifecycleHostError, TransactionLifecycleTurn,
};
pub(crate) use limits::TransactionExecutionLimits;
pub(in crate::transaction) use sequencing::TransactionSendReplacement;

#[cfg(test)]
mod host_support_test;
#[cfg(test)]
mod host_test;
#[cfg(test)]
mod owner_loss_test;
