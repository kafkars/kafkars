//! Bounded ownership of producer records, batch membership, and wire inputs.

mod admission;
mod batch_store;
mod binding;
mod effect;
pub(crate) mod error;
pub(crate) mod execution;
mod host;
mod host_error;
mod interpreter;
pub(crate) mod materialization;
pub(crate) mod prepared;
pub(crate) mod reclaim;
mod reclaim_turn;
pub(crate) mod record;
mod record_access;
mod record_store;
pub(crate) mod store;
mod submission_deadline;
mod topic_catalog;

#[cfg_attr(
    not(test),
    expect(unused_imports, reason = "the facade bridge follows")
)]
pub(crate) use admission::{AdmittedExplicit, ProducerAdmissionFailure, RejectedExplicit};
pub(crate) use binding::{CompletionBindingError, CompletionBindings};
pub(crate) use error::{ProducerAdmissionError, ProducerStoreError};
#[cfg_attr(not(test), expect(unused_imports, reason = "producer host follows"))]
pub(crate) use execution::{PreparedExecution, PreparedExecutionError, PreparedExecutionLimits};
#[cfg_attr(
    not(test),
    expect(unused_imports, reason = "the facade bridge follows")
)]
pub(crate) use host::{ProducerHost, ProducerHostLimits};
pub(crate) use host_error::{
    ProducerHostInvariantError, ProducerHostLimitError, ProducerHostStartError,
    ProducerRejectionReason,
};
pub(crate) use materialization::{
    MaterializationBatch, MaterializationHeader, MaterializationRecord,
};
pub(crate) use record::ProducerRecord;

#[cfg(test)]
pub(crate) use record::ProducerHeader;
pub(crate) use store::{ProducerStore, ProducerStoreLimits, ProducerStoreStats};

#[cfg(test)]
mod admission_test;
#[cfg(test)]
mod batch_store_test;
#[cfg(test)]
mod binding_test;
#[cfg(test)]
mod execution_route_test;
#[cfg(test)]
mod execution_test;
#[cfg(test)]
mod host_limits_test;
#[cfg(test)]
mod interpreter_test;
#[cfg(test)]
mod materialization_test;
#[cfg(test)]
mod prepared_test;
#[cfg(test)]
mod reclaim_test;
#[cfg(test)]
mod reclaim_turn_test;
#[cfg(test)]
mod record_store_test;
#[cfg(test)]
mod submission_deadline_test;
#[cfg(test)]
mod topic_catalog_test;
