//! Exact engine-state replacement for core-authorized definitely-unsent retries.

use kafka_client_core::{BatchExecutionGeneration, BatchExecutionId};

use super::{BatchState, BatchStore};
use crate::producer::ProducerStoreError;

impl BatchStore {
    pub(in crate::producer) fn start_retry(
        &mut self,
        previous: BatchExecutionId,
        replacement: BatchExecutionId,
    ) -> Result<(), ProducerStoreError> {
        if next_execution(previous) != Some(replacement) {
            return Err(ProducerStoreError::StaleBatchExecution);
        }
        let batch = self
            .batches
            .get_mut(&previous.batch_id())
            .ok_or(ProducerStoreError::UnknownBatch)?;
        match batch.state {
            BatchState::Materialized(current) | BatchState::Submitted(current)
                if current == previous =>
            {
                batch.state = BatchState::RetryWaiting(replacement);
                Ok(())
            }
            BatchState::Open
            | BatchState::ReadyForMaterialization(_)
            | BatchState::Materializing(_)
            | BatchState::Materialized(_)
            | BatchState::Submitted(_)
            | BatchState::RetryWaiting(_) => Err(ProducerStoreError::StaleBatchExecution),
        }
    }

    pub(in crate::producer) fn activate_retry(
        &mut self,
        execution: BatchExecutionId,
    ) -> Result<(), ProducerStoreError> {
        let batch = self
            .batches
            .get_mut(&execution.batch_id())
            .ok_or(ProducerStoreError::UnknownBatch)?;
        match batch.state {
            BatchState::Open if execution.generation() == BatchExecutionGeneration::initial() => {
                Ok(())
            }
            BatchState::ReadyForMaterialization(current) if current == execution => Ok(()),
            BatchState::RetryWaiting(current) if current == execution => {
                batch.state = BatchState::ReadyForMaterialization(execution);
                Ok(())
            }
            BatchState::Open
            | BatchState::ReadyForMaterialization(_)
            | BatchState::Materializing(_)
            | BatchState::Materialized(_)
            | BatchState::Submitted(_)
            | BatchState::RetryWaiting(_) => Err(ProducerStoreError::StaleBatchExecution),
        }
    }
}

fn next_execution(previous: BatchExecutionId) -> Option<BatchExecutionId> {
    previous
        .generation()
        .get()
        .checked_add(1)
        .and_then(BatchExecutionGeneration::try_from_raw)
        .map(|generation| BatchExecutionId::new(previous.batch_id(), generation))
}
