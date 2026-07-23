//! Linear materialization attempts spanning canonical views and prepared bytes.

use kafka_client_core::{BatchExecutionId, CompressionPolicy, Moment, ProducerInput};

use super::{PreparedExecution, PreparedExecutionError};
use crate::{
    producer::{
        ProducerStoreError,
        batch_store::{MaterializationAbort, MaterializationAttempt},
        prepared::{PreparedInsertError, PreparedProduceError},
        store::ProducerStore,
    },
    protocol::produce::materialize_explicit_produce_batch,
};

impl PreparedExecution {
    /// Encodes and retains one exact execution before committing materialized state.
    pub(crate) fn materialize(
        &mut self,
        store: &mut ProducerStore,
        execution: BatchExecutionId,
        compression: CompressionPolicy,
        now: Moment,
    ) -> Result<ProducerInput, PreparedExecutionError> {
        match compression {
            CompressionPolicy::Uncompressed => {}
        }
        let (attempt, input) = match store.materialization_view(execution, self.max_batch_bytes) {
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
        match self.prepared.insert(execution, materialized) {
            Ok(()) => self.commit_inserted(store, attempt, now),
            Err(rejected) => {
                let failure = Self::classify_insert_rejection(execution, rejected);
                abort_failed_attempt(store, attempt);
                failure
            }
        }
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
            Err(commit) => match self.prepared.take(execution) {
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
        let _unretained = rejected.into_value();
        match reason {
            PreparedProduceError::BatchCapacity
            | PreparedProduceError::EncodedByteCapacity
            | PreparedProduceError::EncodedByteOverflow => Ok(materialization_failed(execution)),
            PreparedProduceError::DuplicateBatch
            | PreparedProduceError::ExecutionMismatch
            | PreparedProduceError::UnknownBatch => Err(PreparedExecutionError::Prepared(reason)),
        }
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
