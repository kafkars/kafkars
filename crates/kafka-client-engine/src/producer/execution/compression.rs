//! Exact store preparation and host commit for worker materialization.

use kafka_client_core::{
    BatchExecutionId, CompressionPolicy, Moment, ProducerIdentity, ProducerInput,
    ProducerSequenceLease,
};

use super::{PreparedExecution, PreparedExecutionError};
use crate::producer::{
    ProducerHost, ProducerHostInvariantError, ProducerStoreError,
    batch_store::{MaterializationAbort, MaterializationAttempt},
    compression::{CompressionCompletion, CompressionJob},
    store::ProducerStore,
};

impl ProducerHost {
    /// Applies due public deadlines for jobs still owned by compression workers.
    pub(crate) fn fire_due_compression(
        &mut self,
        now: Moment,
        limit: usize,
    ) -> Result<usize, ProducerHostInvariantError> {
        if let Some(error) = self.poison_reason() {
            return Err(error);
        }
        let due = self.compression.drain_due(now, limit);
        let count = due.len();
        for input in due {
            self.apply_generated(now, input)?;
        }
        Ok(count)
    }
}

impl PreparedExecution {
    pub(crate) fn prepare_compression(
        &self,
        store: &mut ProducerStore,
        execution: BatchExecutionId,
        compression: CompressionPolicy,
        identity: ProducerIdentity,
        sequence: ProducerSequenceLease,
    ) -> Result<Result<CompressionJob, ProducerInput>, PreparedExecutionError> {
        let (attempt, input) = match store.materialization_view_idempotent(
            execution,
            self.max_batch_bytes,
            identity,
            sequence,
        ) {
            Ok(view) => view,
            Err(ProducerStoreError::PartitionOutOfRange) => {
                return Ok(Err(materialization_failed(execution)));
            }
            Err(error) => return Err(PreparedExecutionError::Store(error)),
        };
        let execution = attempt.execution();
        match CompressionJob::new(attempt, input, compression) {
            Ok(job) => Ok(Ok(job)),
            Err(attempt) => {
                abort_failed_attempt(store, attempt);
                Ok(Err(materialization_failed(execution)))
            }
        }
    }

    pub(crate) fn complete_compression(
        &mut self,
        store: &mut ProducerStore,
        completion: CompressionCompletion,
        cancelled: bool,
        now: Moment,
    ) -> Result<Option<ProducerInput>, PreparedExecutionError> {
        let execution = completion.execution();
        let (attempt, materialized) = completion.into_parts();
        if cancelled {
            drop((attempt, materialized));
            return Ok(None);
        }
        let Some(materialized) = materialized else {
            abort_failed_attempt(store, attempt);
            return Ok(Some(materialization_failed(execution)));
        };
        match self.insert_materialized(execution, materialized) {
            Ok(()) => self.commit_inserted(store, attempt, now).map(Some),
            Err(rejected) => {
                let failure = Self::classify_insert_rejection(execution, rejected);
                abort_failed_attempt(store, attempt);
                failure.map(Some)
            }
        }
    }
}

fn abort_failed_attempt(store: &mut ProducerStore, attempt: MaterializationAttempt) {
    match store.abort_materialization(attempt) {
        MaterializationAbort::Restored | MaterializationAbort::Superseded => {}
    }
}

const fn materialization_failed(execution: BatchExecutionId) -> ProducerInput {
    ProducerInput::BatchMaterializationFailed { execution }
}
