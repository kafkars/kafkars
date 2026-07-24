//! Linear materialization attempts spanning canonical views and prepared bytes.

use kafka_client_core::{
    BatchExecutionId, CompressionPolicy, Moment, ProducerIdentity, ProducerInput,
    ProducerSequenceLease,
};

use super::{PreparedEntry, PreparedExecution, PreparedExecutionError, PreparedProduceError};
use crate::{
    producer::{
        ProducerStoreError,
        batch_store::{MaterializationAbort, MaterializationAttempt},
        store::ProducerStore,
    },
    protocol::produce::{MaterializedProduce, materialize_explicit_produce_batch},
};

#[derive(Debug)]
pub(super) struct PreparedInsertError {
    reason: PreparedProduceError,
    materialized: MaterializedProduce,
}

impl PreparedExecution {
    /// Encodes and retains one exact execution before committing materialized state.
    pub(crate) fn materialize_idempotent(
        &mut self,
        store: &mut ProducerStore,
        execution: BatchExecutionId,
        compression: CompressionPolicy,
        identity: ProducerIdentity,
        sequence: ProducerSequenceLease,
        now: Moment,
    ) -> Result<ProducerInput, PreparedExecutionError> {
        match compression {
            CompressionPolicy::Uncompressed => {}
        }
        let (attempt, input) = match store.materialization_view_idempotent(
            execution,
            self.max_batch_bytes,
            identity,
            sequence,
        ) {
            Ok(view) => view,
            Err(ProducerStoreError::PartitionOutOfRange) => {
                return Ok(materialization_failed(execution));
            }
            Err(error) => return Err(PreparedExecutionError::Store(error)),
        };
        let execution = attempt.execution();
        let materialized = match materialize_explicit_produce_batch(input) {
            Ok(value) => value,
            Err(_semantic_failure) => {
                abort_failed_attempt(store, attempt);
                return Ok(materialization_failed(execution));
            }
        };
        match self.insert_materialized(execution, materialized) {
            Ok(()) => self.commit_inserted(store, attempt, now),
            Err(rejected) => {
                let failure = Self::classify_insert_rejection(execution, rejected);
                abort_failed_attempt(store, attempt);
                failure
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn materialize(
        &mut self,
        store: &mut ProducerStore,
        execution: BatchExecutionId,
        compression: CompressionPolicy,
        now: Moment,
    ) -> Result<ProducerInput, PreparedExecutionError> {
        let identity =
            ProducerIdentity::try_new(1, 0).ok_or(PreparedExecutionError::UnexpectedEffect)?;
        let sequence = store
            .sequence_for_test(execution)
            .map_err(PreparedExecutionError::Store)?;
        self.materialize_idempotent(store, execution, compression, identity, sequence, now)
    }

    pub(super) fn commit_inserted(
        &mut self,
        store: &mut ProducerStore,
        attempt: MaterializationAttempt,
        now: Moment,
    ) -> Result<ProducerInput, PreparedExecutionError> {
        let execution = attempt.execution();
        match store.commit_materialization(attempt) {
            Ok(()) => Ok(ProducerInput::BatchMaterialized { execution, now }),
            Err(commit) => match self.take_unarmed_materialized(execution) {
                Ok(stale) => {
                    drop(stale);
                    Err(PreparedExecutionError::Store(commit))
                }
                Err(rollback) => Err(PreparedExecutionError::CommitRollback { commit, rollback }),
            },
        }
    }

    fn classify_insert_rejection(
        execution: BatchExecutionId,
        rejected: PreparedInsertError,
    ) -> Result<ProducerInput, PreparedExecutionError> {
        let reason = rejected.reason();
        drop(rejected.materialized);
        match reason {
            PreparedProduceError::BatchCapacity
            | PreparedProduceError::EncodedByteCapacity
            | PreparedProduceError::EncodedByteOverflow => Ok(materialization_failed(execution)),
            PreparedProduceError::DuplicateBatch
            | PreparedProduceError::SubmissionArmed
            | PreparedProduceError::ExecutionMismatch
            | PreparedProduceError::UnknownBatch => Err(PreparedExecutionError::Prepared(reason)),
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "bounded rejection returns the linear materialized request without allocating"
    )]
    pub(super) fn insert_materialized(
        &mut self,
        execution: BatchExecutionId,
        materialized: MaterializedProduce,
    ) -> Result<(), PreparedInsertError> {
        let batch_id = execution.batch_id();
        if let Some(current) = self.entries.get(&batch_id) {
            let reason = if current.execution == execution {
                PreparedProduceError::DuplicateBatch
            } else {
                PreparedProduceError::ExecutionMismatch
            };
            return Err(PreparedInsertError {
                reason,
                materialized,
            });
        }
        if self.entries.len() >= self.batch_capacity {
            return Err(PreparedInsertError {
                reason: PreparedProduceError::BatchCapacity,
                materialized,
            });
        }
        let bytes = materialized.retained_record_bytes();
        let Some(next_bytes) = self.retained_bytes.checked_add(bytes) else {
            return Err(PreparedInsertError {
                reason: PreparedProduceError::EncodedByteOverflow,
                materialized,
            });
        };
        if next_bytes > self.encoded_byte_capacity {
            return Err(PreparedInsertError {
                reason: PreparedProduceError::EncodedByteCapacity,
                materialized,
            });
        }
        self.entries.insert(
            batch_id,
            PreparedEntry {
                execution,
                materialized,
                submission: None,
            },
        );
        self.retained_bytes = next_bytes;
        Ok(())
    }

    pub(super) fn take_unarmed_materialized(
        &mut self,
        execution: BatchExecutionId,
    ) -> Result<MaterializedProduce, PreparedProduceError> {
        let entry = self
            .entries
            .get(&execution.batch_id())
            .ok_or(PreparedProduceError::UnknownBatch)?;
        if entry.execution != execution {
            return Err(PreparedProduceError::ExecutionMismatch);
        }
        if entry.submission.is_some()
            || self
                .schedule
                .iter()
                .any(|scheduled| scheduled.execution.batch_id() == execution.batch_id())
        {
            return Err(PreparedProduceError::SubmissionArmed);
        }
        let bytes = entry.materialized.retained_record_bytes();
        let next_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(PreparedProduceError::EncodedByteOverflow)?;
        let entry = self
            .entries
            .remove(&execution.batch_id())
            .ok_or(PreparedProduceError::UnknownBatch)?;
        self.retained_bytes = next_bytes;
        Ok(entry.materialized)
    }

    #[cfg(test)]
    pub(super) fn retain_for_test(
        &mut self,
        execution: BatchExecutionId,
        materialized: MaterializedProduce,
    ) -> Result<(), PreparedProduceError> {
        self.insert_materialized(execution, materialized)
            .map_err(|rejected| rejected.reason)
    }
}

impl PreparedInsertError {
    pub(super) const fn reason(&self) -> PreparedProduceError {
        self.reason
    }

    pub(super) fn into_materialized(self) -> MaterializedProduce {
        self.materialized
    }
}

fn abort_failed_attempt(store: &mut ProducerStore, attempt: MaterializationAttempt) {
    match store.abort_materialization(attempt) {
        // Restored retries remain exact. Superseded attempts report the old
        // execution, which core filters without disturbing the replacement.
        MaterializationAbort::Restored | MaterializationAbort::Superseded => {}
    }
}

const fn materialization_failed(execution: BatchExecutionId) -> ProducerInput {
    ProducerInput::BatchMaterializationFailed { execution }
}
