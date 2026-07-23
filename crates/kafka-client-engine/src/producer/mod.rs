//! Bounded ownership of producer records, batch membership, and wire inputs.

mod batch_store;
mod binding;
pub(crate) mod error;
pub(crate) mod materialization;
pub(crate) mod prepared;
pub(crate) mod reclaim;
pub(crate) mod record;
mod record_access;
mod record_store;
pub(crate) mod store;
mod submission_deadline;
mod topic_catalog;

pub(crate) use binding::{CompletionBindingError, CompletionBindings};
pub(crate) use error::{ProducerAdmissionError, ProducerStoreError};
pub(crate) use materialization::{
    MaterializationBatch, MaterializationHeader, MaterializationRecord,
};
pub(crate) use record::ProducerRecord;
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "submission deadlines precede the integrated producer host"
    )
)]
pub(crate) use submission_deadline::{
    DueSubmissionDeadline, SubmissionDeadlineError, SubmissionDeadlines,
};

#[cfg(test)]
pub(crate) use record::ProducerHeader;
#[cfg(test)]
pub(crate) use store::{ProducerStore, ProducerStoreLimits, ProducerStoreStats};

#[cfg(test)]
mod batch_store_test;
#[cfg(test)]
mod binding_test;
#[cfg(test)]
mod materialization_test;
#[cfg(test)]
mod prepared_test;
#[cfg(test)]
mod reclaim_test;
#[cfg(test)]
mod record_store_test;
#[cfg(test)]
mod submission_deadline_test;
#[cfg(test)]
mod topic_catalog_test;
