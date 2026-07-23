//! Prepared-execution invariant failures and their stable diagnostics.

use std::{error::Error, fmt};

use kafka_client_core::{
    BatchExecutionId, BatchId, Deadline, OperationId, PartitionIndex, TopicId,
};

use super::{PreparedProduceError, PreparedRevisionExpectation, SubmissionDeadlineError};
use crate::producer::ProducerStoreError;

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
        /// Exact execution retaining the unified prepared entry.
        retained: Option<BatchExecutionId>,
    },
    /// Cancellation expected a different prepared-byte lifecycle phase.
    RevisionStateMismatch {
        /// Exact execution being revoked.
        execution: BatchExecutionId,
        /// Required prepared-byte phase.
        expected: PreparedRevisionExpectation,
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
                retained,
            } => write!(
                formatter,
                "batch {} cleanup execution mismatch: expected {expected:?}, retained \
                 {retained:?}",
                batch_id.get()
            ),
            Self::RevisionStateMismatch {
                execution,
                expected,
            } => write!(
                formatter,
                "batch {} generation {} prepared revision expected {expected:?}",
                execution.batch_id().get(),
                execution.generation().get()
            ),
        }
    }
}

impl Error for PreparedExecutionError {}
