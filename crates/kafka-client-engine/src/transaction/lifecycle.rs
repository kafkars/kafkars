//! Declarative boundary for private transactional lifecycle execution.

mod admission;
mod completion;
mod driver_port;
mod effect;
mod host;
mod port;
mod recovery;
mod start;
mod turn;

pub(crate) use host::{
    TransactionLifecycleHost, TransactionLifecycleHostError, TransactionLifecycleTurn,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod owner_loss_test;
