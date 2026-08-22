//! Exact replacement of virtual attempts after core-authorized retry.

use kafka_client_core::{BatchExecutionGeneration, BatchExecutionId};

use super::{
    VirtualProducerState,
    batch::{VirtualBatch, VirtualBatchPhase},
};
use crate::SimulationError;

impl VirtualBatch {
    pub(super) fn start_retry(
        &mut self,
        previous: BatchExecutionId,
        replacement: BatchExecutionId,
    ) -> Result<(), SimulationError> {
        let current = match self.phase {
            VirtualBatchPhase::AwaitingDriver(current) | VirtualBatchPhase::Submitted(current) => {
                current
            }
            VirtualBatchPhase::Ready(current)
            | VirtualBatchPhase::Materializing(current)
            | VirtualBatchPhase::Materialized(current)
            | VirtualBatchPhase::RetryWaiting(current) => {
                return Err(SimulationError::BatchExecutionMismatch {
                    expected: Some(current),
                    actual: previous,
                });
            }
            VirtualBatchPhase::Open => {
                return Err(SimulationError::BatchExecutionMismatch {
                    expected: None,
                    actual: previous,
                });
            }
        };
        let expected = next_execution(previous);
        if current != previous || expected != Some(replacement) {
            return Err(SimulationError::BatchExecutionMismatch {
                expected,
                actual: replacement,
            });
        }
        self.phase = VirtualBatchPhase::RetryWaiting(replacement);
        Ok(())
    }

    pub(super) fn activate_retry(
        &mut self,
        execution: BatchExecutionId,
    ) -> Result<(), SimulationError> {
        match self.phase {
            VirtualBatchPhase::RetryWaiting(current) if current == execution => {
                self.phase = VirtualBatchPhase::Ready(execution);
                Ok(())
            }
            VirtualBatchPhase::RetryWaiting(current) => {
                Err(SimulationError::BatchExecutionMismatch {
                    expected: Some(current),
                    actual: execution,
                })
            }
            VirtualBatchPhase::Open
            | VirtualBatchPhase::Ready(_)
            | VirtualBatchPhase::Materializing(_)
            | VirtualBatchPhase::Materialized(_)
            | VirtualBatchPhase::AwaitingDriver(_)
            | VirtualBatchPhase::Submitted(_) => Ok(()),
        }
    }
}

impl VirtualProducerState {
    pub(super) fn retry_batch_execution(
        &mut self,
        previous: BatchExecutionId,
        replacement: BatchExecutionId,
    ) -> Result<(), SimulationError> {
        self.batches
            .get_mut(&previous.batch_id())
            .ok_or(SimulationError::UnknownBatch(previous.batch_id()))?
            .start_retry(previous, replacement)
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
