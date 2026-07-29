//! Declarative private bridge for one partition-transaction abort.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminAbortPartitionTransaction;
pub(crate) use request::AbortPartitionTransactionAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
