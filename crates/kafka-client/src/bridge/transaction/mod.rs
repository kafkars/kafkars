//! Declarative private bridge for transaction initialization and owner lifetime.

mod handle;
mod lifecycle;
mod operation;
mod owner;
mod result;

pub(crate) use handle::TransactionalProducerInitializer;
pub(crate) use lifecycle::{TransactionEndEngine, TransactionEngine};
pub(crate) use operation::TransactionInitialization;
pub(crate) use owner::TransactionalProducerEngine;

#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod lifecycle_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod owner_test;
#[cfg(test)]
mod result_test;
