//! Bounded execution of core-declared producer materialization and submission.

use std::{error::Error, fmt};

use kafka_client_core::{
    AcknowledgementPolicy, BatchId, CompressionPolicy, Deadline, Moment, PartitionIndex,
    ProducerEffect, ProducerInput, TopicId,
};

use super::{
    ProducerStoreError,
    prepared::{PreparedProduceError, PreparedProduceStats, PreparedProduceStore},
    store::ProducerStore,
    submission_deadline::{SubmissionDeadlineError, SubmissionDeadlines},
};
use crate::{
    producer::prepared::PreparedInsertError, protocol::produce::materialize_explicit_produce_batch,
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
    MissingPreparedBatch(BatchId),
    /// A submission effect disagreed with retained engine route provenance.
    RouteMismatch {
        /// Batch whose route facts diverged.
        batch_id: BatchId,
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
}

impl fmt::Display for PreparedExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEffect => {
                formatter.write_str("prepared execution received a non-submission effect")
            }
            Self::Store(error) => write!(formatter, "producer store execution failed: {error}"),
            Self::MissingPreparedBatch(batch_id) => write!(
                formatter,
                "batch {} has no prepared Produce bytes",
                batch_id.get()
            ),
            Self::RouteMismatch { batch_id, .. } => write!(
                formatter,
                "batch {} submission route disagrees with retained provenance",
                batch_id.get()
            ),
            Self::Prepared(error) => {
                write!(formatter, "prepared Produce ownership failed: {error}")
            }
            Self::Deadline(error) => write!(formatter, "submission deadline failed: {error}"),
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

    /// Executes wire-records materialization and returns one deferred core fact.
    pub(crate) fn materialize(
        &mut self,
        store: &mut ProducerStore,
        batch_id: BatchId,
        compression: CompressionPolicy,
        now: Moment,
    ) -> Result<ProducerInput, PreparedExecutionError> {
        match compression {
            CompressionPolicy::Uncompressed => {}
        }
        let input = match store.materialization_view(batch_id, self.max_batch_bytes) {
            Ok(input) => input,
            Err(ProducerStoreError::PartitionOutOfRange) => {
                return Ok(materialization_failed(batch_id));
            }
            Err(error) => return Err(PreparedExecutionError::Store(error)),
        };
        let materialized = match materialize_explicit_produce_batch(input) {
            Ok(value) => value,
            Err(_semantic_failure) => return Ok(materialization_failed(batch_id)),
        };
        match self.prepared.insert(batch_id, materialized) {
            Ok(()) => Ok(ProducerInput::BatchMaterialized { batch_id, now }),
            Err(rejected) => Self::classify_insert_rejection(batch_id, rejected),
        }
    }

    /// Retains the exact core-selected deadline while bytes await the driver.
    pub(crate) fn arm_submission(
        &mut self,
        store: &ProducerStore,
        effect: ProducerEffect,
    ) -> Result<(), PreparedExecutionError> {
        let ProducerEffect::SubmitProduce {
            batch_id,
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
        if !self.prepared.contains(batch_id) {
            return Err(PreparedExecutionError::MissingPreparedBatch(batch_id));
        }
        let (stored_topic_id, stored_partition) = store
            .batch_route(batch_id)
            .map_err(PreparedExecutionError::Store)?;
        if stored_topic_id != topic_id || stored_partition != partition {
            return Err(PreparedExecutionError::RouteMismatch {
                batch_id,
                stored_topic_id,
                stored_partition,
                effect_topic_id: topic_id,
                effect_partition: partition,
            });
        }
        self.deadlines
            .arm(batch_id, deadline_operation_id, deadline)
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

    /// Releases encoded bytes, deadline ownership, and original batch membership.
    pub(crate) fn release_batch(
        &mut self,
        store: &mut ProducerStore,
        batch_id: BatchId,
    ) -> Result<(), PreparedExecutionError> {
        store
            .release_batch(batch_id)
            .map_err(PreparedExecutionError::Store)?;
        let _cancelled = self.deadlines.cancel(batch_id);
        self.prepared
            .release_if_present(batch_id)
            .map(|_released| ())
            .map_err(PreparedExecutionError::Prepared)
    }

    /// Returns bounded prepared-byte ownership for metrics and host checks.
    pub(crate) fn prepared_stats(&self) -> PreparedProduceStats {
        self.prepared.stats()
    }

    /// Returns active batches that have not crossed driver ownership.
    pub(crate) fn submission_count(&self) -> usize {
        self.deadlines.len()
    }

    /// Drops encoded requests and deadline ownership terminally.
    pub(crate) fn clear_terminal(&mut self) {
        self.prepared.clear_terminal();
        self.deadlines.clear_terminal();
    }

    fn classify_insert_rejection(
        batch_id: BatchId,
        rejected: PreparedInsertError,
    ) -> Result<ProducerInput, PreparedExecutionError> {
        let reason = rejected.reason();
        let _unretained = rejected.into_value();
        match reason {
            PreparedProduceError::BatchCapacity
            | PreparedProduceError::EncodedByteCapacity
            | PreparedProduceError::EncodedByteOverflow => Ok(materialization_failed(batch_id)),
            PreparedProduceError::DuplicateBatch | PreparedProduceError::UnknownBatch => {
                Err(PreparedExecutionError::Prepared(reason))
            }
        }
    }
}

const fn materialization_failed(batch_id: BatchId) -> ProducerInput {
    ProducerInput::BatchMaterializationFailed { batch_id }
}
