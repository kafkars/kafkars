//! Bounded ownership of producer records, batch membership, and wire inputs.
mod admission;
mod batch_store;
mod binding;
mod boundary;
mod effect;
pub(crate) mod error;
pub(crate) mod execution;
pub(crate) mod execution_stop;
mod execution_turn;
mod host;
mod host_error;
pub(crate) mod host_turn;
pub(crate) mod ingress;
mod interpreter;
pub(crate) mod materialization;
pub(crate) mod reclaim;
mod reclaim_turn;
pub(crate) mod record;
mod record_access;
mod record_store;
pub(crate) mod shutdown;
pub(crate) mod store;
mod terminal;
mod terminal_backlog;
mod terminal_publication;
mod topic_catalog;
pub use boundary::*;
pub(crate) use error::{ProducerAdmissionError, ProducerStoreError};
pub(crate) use host::{ProducerHost, ProducerHostLimits};
pub(crate) use host_error::{ProducerHostInvariantError, ProducerHostLimitError};
pub(crate) use host_error::{ProducerHostStartError, ProducerRejectionReason};
pub(crate) use record::ProducerRecord;
pub(crate) use store::{ProducerStore, ProducerStoreLimits, ProducerStoreStats};
pub(crate) use terminal::ProducerTerminal;
#[cfg(test)]
mod admission_test;
#[cfg(test)]
mod batch_store_test;
#[cfg(test)]
mod binding_test;
#[cfg(test)]
mod execution_route_test;
#[cfg(test)]
mod execution_stop_test;
#[cfg(test)]
mod execution_test;
#[cfg(test)]
mod execution_turn_test;
#[cfg(test)]
mod host_limits_test;
#[cfg(test)]
mod host_turn_test;
#[cfg(test)]
mod interpreter_test;
#[cfg(test)]
mod materialization_retention_test;
#[cfg(test)]
mod materialization_test;
#[cfg(test)]
mod reclaim_test;
#[cfg(test)]
mod reclaim_turn_test;
#[cfg(test)]
mod record_store_test;
#[cfg(test)]
mod record_test;
#[cfg(test)]
mod shutdown_test;
#[cfg(test)]
mod terminal_backlog_test;
#[cfg(test)]
mod terminal_publication_test;
#[cfg(test)]
mod terminal_test;
#[cfg(test)]
mod topic_catalog_test;
