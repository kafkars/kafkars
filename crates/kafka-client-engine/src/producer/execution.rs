//! Bounded execution of core-declared producer materialization and submission.

mod cleanup;
#[cfg(test)]
mod cleanup_test;
#[cfg(test)]
mod deadline_test;
mod error;
#[cfg(test)]
mod error_test;
mod handoff;
#[cfg(test)]
mod handoff_test;
mod materialization;
#[cfg(test)]
mod materialization_test;
mod next_submission;
#[cfg(test)]
mod next_submission_test;
mod ownership;
mod revision;
#[cfg(test)]
mod revision_test;
mod submission;
#[cfg(test)]
mod submission_test;

use std::collections::{BTreeMap, BTreeSet};

use kafka_client_core::{BatchExecutionId, BatchId, Deadline, OperationId};

use crate::{clock::OperationDeadline, protocol::produce::MaterializedProduce};

pub(crate) use error::PreparedExecutionError;
pub(crate) use handoff::{PreparedProduceHandoffError, PreparedProduceSubmission};
pub(crate) use ownership::{PreparedProduceError, PreparedProduceStats, SubmissionDeadlineError};
pub(crate) use revision::{PreparedRevisionExpectation, PreparedRevisionPlan};

/// Hard bounds shared by encoded bytes and pre-driver deadline ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PreparedExecutionLimits {
    /// Maximum encoded `RecordBatch` bytes retained before driver acceptance.
    pub(crate) encoded_bytes: usize,
    /// Per-batch limit passed to the authoritative wire-records encoder.
    pub(crate) max_batch_bytes: usize,
}

/// Single bounded owner of materialized bytes awaiting real driver acceptance.
#[derive(Debug)]
pub(crate) struct PreparedExecution {
    max_batch_bytes: usize,
    batch_capacity: usize,
    encoded_byte_capacity: usize,
    retained_bytes: usize,
    entries: BTreeMap<BatchId, PreparedEntry>,
    schedule: BTreeSet<ScheduledDeadline>,
}

#[derive(Debug)]
struct PreparedEntry {
    execution: BatchExecutionId,
    materialized: MaterializedProduce,
    submission: Option<SubmissionFacts>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SubmissionFacts {
    operation_id: OperationId,
    deadline: OperationDeadline,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ScheduledDeadline {
    deadline: Deadline,
    execution: BatchExecutionId,
}

impl PreparedExecution {
    /// Uses one host-validated batch capacity for bytes and deadline ownership.
    pub(crate) const fn new(batch_capacity: usize, limits: PreparedExecutionLimits) -> Self {
        Self {
            max_batch_bytes: limits.max_batch_bytes,
            batch_capacity,
            encoded_byte_capacity: limits.encoded_bytes,
            retained_bytes: 0,
            entries: BTreeMap::new(),
            schedule: BTreeSet::new(),
        }
    }

    /// Returns the next unchanged core deadline for host-turn scheduling.
    pub(crate) fn next_deadline(&self) -> Option<Deadline> {
        self.schedule.first().map(|entry| entry.deadline)
    }

    /// Returns bounded prepared-byte ownership for metrics and host checks.
    pub(crate) fn prepared_stats(&self) -> PreparedProduceStats {
        PreparedProduceStats {
            batches: self.entries.len(),
            encoded_record_bytes: self.retained_bytes,
        }
    }

    /// Returns active batches that have not crossed driver ownership.
    pub(crate) fn submission_count(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| entry.submission.is_some())
            .count()
    }
}
