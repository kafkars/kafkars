//! Preflighted exact cleanup of batch membership, prepared bytes, and deadlines.

use kafka_client_core::BatchId;

use super::{PreparedExecution, PreparedExecutionError};
use crate::producer::store::ProducerStore;

impl PreparedExecution {
    /// Releases every owner only after exact-generation preflight succeeds.
    pub(crate) fn release_batch(
        &mut self,
        store: &mut ProducerStore,
        batch_id: BatchId,
    ) -> Result<(), PreparedExecutionError> {
        let expected = store
            .batch_execution(batch_id)
            .map_err(PreparedExecutionError::Store)?;
        let prepared = self.prepared.execution(batch_id);
        let deadline = self.deadlines.execution(batch_id);
        if prepared.is_some_and(|actual| Some(actual) != expected)
            || deadline.is_some_and(|actual| Some(actual) != expected)
        {
            return Err(PreparedExecutionError::CleanupExecutionMismatch {
                batch_id,
                expected,
                prepared,
                deadline,
            });
        }
        let prepared_retained = match expected {
            Some(execution) => self
                .prepared
                .preflight_release(execution)
                .map_err(PreparedExecutionError::Prepared)?,
            None => false,
        };
        store
            .release_batch(batch_id)
            .map_err(PreparedExecutionError::Store)?;
        if let Some(execution) = expected {
            let cancelled = self.deadlines.cancel(execution);
            if deadline.is_some() != cancelled {
                return Err(PreparedExecutionError::CleanupExecutionMismatch {
                    batch_id,
                    expected,
                    prepared,
                    deadline,
                });
            }
            if prepared_retained {
                self.prepared
                    .release(execution)
                    .map(|_released| ())
                    .map_err(PreparedExecutionError::Prepared)?;
            }
        }
        Ok(())
    }

    /// Drops all execution mechanisms during outside-in terminal recovery.
    pub(crate) fn clear_terminal(&mut self) {
        self.prepared.clear_terminal();
        self.deadlines.clear_terminal();
    }
}
