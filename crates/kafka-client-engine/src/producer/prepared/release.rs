//! Exact-generation preflight and removal of retained prepared requests.

use kafka_client_core::{BatchExecutionId, BatchId};

use super::{PreparedProduceError, PreparedProduceStore};
use crate::protocol::produce::MaterializedProduce;

impl PreparedProduceStore {
    /// Transfers one request and its accounting out to the synchronous host.
    pub(crate) fn take(
        &mut self,
        execution: BatchExecutionId,
    ) -> Result<MaterializedProduce, PreparedProduceError> {
        self.remove(execution)
    }

    /// Drops one retained request and releases its accounting exactly once.
    pub(crate) fn release(
        &mut self,
        execution: BatchExecutionId,
    ) -> Result<usize, PreparedProduceError> {
        let value = self.remove(execution)?;
        Ok(value.retained_record_bytes())
    }

    /// Preflights an optional exact release without mutating byte ownership.
    pub(crate) fn preflight_release(
        &self,
        execution: BatchExecutionId,
    ) -> Result<bool, PreparedProduceError> {
        let Some(entry) = self.batches.get(&execution.batch_id()) else {
            return Ok(false);
        };
        if entry.execution != execution {
            return Err(PreparedProduceError::ExecutionMismatch);
        }
        self.retained_bytes
            .checked_sub(entry.value.retained_record_bytes())
            .ok_or(PreparedProduceError::EncodedByteOverflow)?;
        Ok(true)
    }

    /// Returns the exact retained execution for cleanup preflight.
    pub(crate) fn execution(&self, batch_id: BatchId) -> Option<BatchExecutionId> {
        self.batches.get(&batch_id).map(|entry| entry.execution)
    }

    fn remove(
        &mut self,
        execution: BatchExecutionId,
    ) -> Result<MaterializedProduce, PreparedProduceError> {
        let entry = self
            .batches
            .get(&execution.batch_id())
            .ok_or(PreparedProduceError::UnknownBatch)?;
        if entry.execution != execution {
            return Err(PreparedProduceError::ExecutionMismatch);
        }
        let bytes = entry.value.retained_record_bytes();
        let next_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(PreparedProduceError::EncodedByteOverflow)?;
        let entry = self
            .batches
            .remove(&execution.batch_id())
            .ok_or(PreparedProduceError::UnknownBatch)?;
        self.retained_bytes = next_bytes;
        Ok(entry.value)
    }
}
