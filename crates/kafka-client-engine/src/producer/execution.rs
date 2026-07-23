//! Bounded execution of core-declared producer materialization and submission.

mod cleanup;
#[cfg(test)]
mod cleanup_test;
mod handoff;
#[cfg(test)]
mod handoff_test;
mod materialization;
#[cfg(test)]
mod materialization_test;
mod next_submission;
#[cfg(test)]
mod next_submission_test;
mod submission;
#[cfg(test)]
mod submission_test;

use std::{error::Error, fmt};

use kafka_client_core::{
    BatchExecutionId, BatchId, Deadline, Moment, OperationId, PartitionIndex, ProducerInput,
    TopicId,
};

use super::{
    ProducerStoreError,
    prepared::{PreparedProduceError, PreparedProduceStats, PreparedProduceStore},
    submission_deadline::{SubmissionDeadlineError, SubmissionDeadlines},
};

/// Hard bounds shared by encoded bytes and pre-driver deadline ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PreparedExecutionLimits {
    /// Maximum encoded `RecordBatch` bytes retained before driver acceptance.
    pub(crate) encoded_bytes: usize,
    /// Per-batch limit passed to the authoritative wire-records encoder.
    pub(crate) max_batch_bytes: usize,
}

/// Failure indicating engine ownership drift rather than a semantic batch failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedExecutionError {
    /// The execution entrypoint received an effect owned by another mechanism.
    UnexpectedEffect,
    /// The original payload or batch store disagreed with a core effect.
    Store(ProducerStoreError),
    /// A submission effect named a batch without retained encoded bytes.
    MissingPreparedBatch(BatchExecutionId),
    /// A submission effect disagreed with retained engine route provenance.
    RouteMismatch {
        /// Batch whose route facts diverged.
        execution: BatchExecutionId,
        /// Topic identity retained when the record was admitted.
        stored_topic_id: TopicId,
        /// Explicit partition retained when the record was admitted.
        stored_partition: PartitionIndex,
        /// Topic identity named by deterministic core.
        effect_topic_id: TopicId,
        /// Explicit partition named by deterministic core.
        effect_partition: PartitionIndex,
    },
    /// Encoded request ownership was internally inconsistent.
    Prepared(PreparedProduceError),
    /// Core-declared deadline ownership was internally inconsistent.
    Deadline(SubmissionDeadlineError),
    /// A core submission effect named no live admitted deadline owner.
    UnknownDeadlineOperation(OperationId),
    /// A core submission effect disagreed with the admitted operation deadline.
    DeadlineMismatch {
        /// Operation selected by core as the batch deadline owner.
        operation_id: OperationId,
        /// Core deadline emitted in the submission effect.
        effect: Deadline,
        /// Core deadline retained from the original public boundary.
        bound: Deadline,
    },
    /// The selected deadline operation belongs to another materialized batch.
    DeadlineOperationMismatch {
        /// Exact batch execution being armed.
        execution: BatchExecutionId,
        /// Operation selected by core as the deadline owner.
        operation_id: OperationId,
    },
    /// A stale commit could not remove the exact prepared bytes it inserted.
    CommitRollback {
        /// Commit failure that detected the phase race.
        commit: ProducerStoreError,
        /// Exact prepared-byte rollback failure.
        rollback: PreparedProduceError,
    },
    /// Terminal cleanup owners disagree about the exact retained execution.
    CleanupExecutionMismatch {
        /// Logical batch being released.
        batch_id: BatchId,
        /// Exact execution retained with canonical membership.
        expected: Option<BatchExecutionId>,
        /// Exact execution retaining prepared bytes.
        prepared: Option<BatchExecutionId>,
        /// Exact execution retaining a pre-driver deadline.
        deadline: Option<BatchExecutionId>,
    },
}

impl fmt::Display for PreparedExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEffect => {
                formatter.write_str("prepared execution received a non-submission effect")
            }
            Self::Store(error) => write!(formatter, "producer store execution failed: {error}"),
            Self::MissingPreparedBatch(execution) => write!(
                formatter,
                "batch {} generation {} has no prepared Produce bytes",
                execution.batch_id().get(),
                execution.generation().get()
            ),
            Self::RouteMismatch { execution, .. } => write!(
                formatter,
                "batch {} generation {} submission route disagrees with retained provenance",
                execution.batch_id().get(),
                execution.generation().get()
            ),
            Self::Prepared(error) => {
                write!(formatter, "prepared Produce ownership failed: {error}")
            }
            Self::Deadline(error) => write!(formatter, "submission deadline failed: {error}"),
            Self::UnknownDeadlineOperation(operation_id) => write!(
                formatter,
                "submission deadline operation {} has no live engine binding",
                operation_id.get()
            ),
            Self::DeadlineMismatch { operation_id, .. } => write!(
                formatter,
                "submission deadline for operation {} disagrees with admission",
                operation_id.get()
            ),
            Self::DeadlineOperationMismatch {
                execution,
                operation_id,
            } => write!(
                formatter,
                "submission deadline operation {} does not belong to batch {} generation {}",
                operation_id.get(),
                execution.batch_id().get(),
                execution.generation().get()
            ),
            Self::CommitRollback { commit, rollback } => write!(
                formatter,
                "materialization commit failed: {commit}; exact prepared rollback failed: \
                 {rollback}"
            ),
            Self::CleanupExecutionMismatch {
                batch_id,
                expected,
                prepared,
                deadline,
            } => write!(
                formatter,
                "batch {} cleanup execution mismatch: expected {expected:?}, prepared \
                 {prepared:?}, deadline {deadline:?}",
                batch_id.get()
            ),
        }
    }
}

impl Error for PreparedExecutionError {}

/// Single bounded owner of materialized bytes awaiting real driver acceptance.
#[derive(Debug)]
pub(crate) struct PreparedExecution {
    max_batch_bytes: usize,
    prepared: PreparedProduceStore,
    deadlines: SubmissionDeadlines,
}

impl PreparedExecution {
    /// Uses one host-validated batch capacity for bytes and deadline ownership.
    pub(crate) const fn new(batch_capacity: usize, limits: PreparedExecutionLimits) -> Self {
        Self {
            max_batch_bytes: limits.max_batch_bytes,
            prepared: PreparedProduceStore::new(batch_capacity, limits.encoded_bytes),
            deadlines: SubmissionDeadlines::new(batch_capacity),
        }
    }

    /// Converts bounded due mechanism entries into FIFO deterministic facts.
    pub(crate) fn drain_due(&mut self, now: Moment, limit: usize) -> Vec<ProducerInput> {
        self.deadlines
            .drain_due(now, limit)
            .into_iter()
            .map(super::submission_deadline::DueSubmissionDeadline::into_input)
            .collect()
    }

    /// Returns the next unchanged core deadline for host-turn scheduling.
    pub(crate) fn next_deadline(&self) -> Option<Deadline> {
        self.deadlines.next_deadline()
    }

    /// Returns bounded prepared-byte ownership for metrics and host checks.
    pub(crate) fn prepared_stats(&self) -> PreparedProduceStats {
        self.prepared.stats()
    }

    /// Returns active batches that have not crossed driver ownership.
    pub(crate) fn submission_count(&self) -> usize {
        self.deadlines.len()
    }
}
