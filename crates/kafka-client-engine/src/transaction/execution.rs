//! Declarative facade for one initialized transaction lifecycle.

mod host;
mod turn;

pub(crate) use host::TransactionExecutionHost;
