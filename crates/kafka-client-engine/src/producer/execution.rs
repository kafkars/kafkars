//! Bounded execution of core-declared producer materialization and submission.

mod cleanup;
#[cfg(test)]
mod cleanup_test;
mod materialization;
#[cfg(test)]
mod materialization_test;

use std::{error::Error, fmt};

use kafka_client_core::{
    AcknowledgementPolicy, BatchExecutionId, BatchId, Deadline, Moment, PartitionIndex,
    ProducerEffect, ProducerInput, TopicId,
};

use super::{
    ProducerStoreError,
    prepared::{PreparedProduceError, PreparedProduceStats, PreparedProduceStore},
    store::ProducerStore,
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

    /// Retains the exact core-selected deadline while bytes await the driver.
    pub(crate) fn arm_submission(
        &mut self,
        store: &ProducerStore,
        effect: ProducerEffect,
    ) -> Result<(), PreparedExecutionError> {
        let ProducerEffect::SubmitProduce {
            execution,
            deadline_operation_id,
            deadline,
            topic_id,
            partition,
            acknowledgements,
        } = effect
        else {
            return Err(PreparedExecutionError::UnexpectedEffect);
        };
        match acknowledgements {
            AcknowledgementPolicy::All => {}
        }
        if !self.prepared.contains(execution) {
            return Err(PreparedExecutionError::MissingPreparedBatch(execution));
        }
        let (stored_topic_id, stored_partition) = store
            .execution_route(execution)
            .map_err(PreparedExecutionError::Store)?;
        if stored_topic_id != topic_id || stored_partition != partition {
            return Err(PreparedExecutionError::RouteMismatch {
                execution,
                stored_topic_id,
                stored_partition,
                effect_topic_id: topic_id,
                effect_partition: partition,
            });
        }
        self.deadlines
            .arm(execution, deadline_operation_id, deadline)
            .map(|_newly_armed| ())
            .map_err(PreparedExecutionError::Deadline)
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
