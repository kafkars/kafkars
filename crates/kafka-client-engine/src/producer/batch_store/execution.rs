//! Exact-generation ownership of sealed batch materialization phases.

use kafka_client_core::{BatchExecutionGeneration, BatchExecutionId};

use super::{BatchAccumulator, BatchPlan, BatchState, BatchStore};
use crate::producer::ProducerStoreError;

/// Linear ownership of one exact in-progress materialization.
#[derive(Debug)]
#[must_use = "a materialization attempt must be committed or explicitly aborted"]
pub(crate) struct MaterializationAttempt {
    execution: BatchExecutionId,
}

/// Outcome of consuming a materialization attempt without committing it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MaterializationAbort {
    /// The exact sealed execution returned to the ready phase.
    Restored,
    /// A replacement execution already owns the batch.
    Superseded,
}

impl MaterializationAttempt {
    /// Returns the exact sealed membership owned by this attempt.
    pub(crate) const fn execution(&self) -> BatchExecutionId {
        self.execution
    }

    const fn into_execution(self) -> BatchExecutionId {
        self.execution
    }
}

impl BatchAccumulator {
    fn seal(&mut self, execution: BatchExecutionId) -> Result<(), ProducerStoreError> {
        match self.state {
            BatchState::Open => {
                if execution.generation() != BatchExecutionGeneration::initial() {
                    return Err(ProducerStoreError::StaleBatchExecution);
                }
                self.state = BatchState::ReadyForMaterialization(execution);
                Ok(())
            }
            BatchState::ReadyForMaterialization(current) if current == execution => Ok(()),
            BatchState::ReadyForMaterialization(_)
            | BatchState::Materializing(_)
            | BatchState::Materialized(_)
            | BatchState::Submitted(_) => Err(ProducerStoreError::StaleBatchExecution),
        }
    }

    fn begin(&mut self, execution: BatchExecutionId) -> Result<(), ProducerStoreError> {
        match self.state {
            BatchState::ReadyForMaterialization(current) if current == execution => {
                self.state = BatchState::Materializing(execution);
                Ok(())
            }
            BatchState::ReadyForMaterialization(_) => Err(ProducerStoreError::StaleBatchExecution),
            BatchState::Open
            | BatchState::Materializing(_)
            | BatchState::Materialized(_)
            | BatchState::Submitted(_) => Err(ProducerStoreError::BatchAlreadyMaterialized),
        }
    }

    fn commit(&mut self, execution: BatchExecutionId) -> Result<(), ProducerStoreError> {
        match self.state {
            BatchState::Materializing(current) if current == execution => {
                self.state = BatchState::Materialized(execution);
                Ok(())
            }
            BatchState::ReadyForMaterialization(current)
            | BatchState::Materializing(current)
            | BatchState::Materialized(current)
            | BatchState::Submitted(current)
                if current != execution =>
            {
                Err(ProducerStoreError::StaleBatchExecution)
            }
            BatchState::Open
            | BatchState::ReadyForMaterialization(_)
            | BatchState::Materialized(_)
            | BatchState::Submitted(_) => Err(ProducerStoreError::BatchAlreadyMaterialized),
            BatchState::Materializing(_) => Err(ProducerStoreError::StaleBatchExecution),
        }
    }

    fn abort(&mut self, execution: BatchExecutionId) -> bool {
        if self.state == BatchState::Materializing(execution) {
            self.state = BatchState::ReadyForMaterialization(execution);
            true
        } else {
            false
        }
    }

    fn execution(&self) -> Option<BatchExecutionId> {
        match self.state {
            BatchState::Open => None,
            BatchState::ReadyForMaterialization(execution)
            | BatchState::Materializing(execution)
            | BatchState::Materialized(execution)
            | BatchState::Submitted(execution) => Some(execution),
        }
    }
}

impl BatchStore {
    pub(in crate::producer) fn seal_for_materialization(
        &mut self,
        execution: BatchExecutionId,
    ) -> Result<(), ProducerStoreError> {
        let batch = self
            .batches
            .get_mut(&execution.batch_id())
            .ok_or(ProducerStoreError::UnknownBatch)?;
        batch.seal(execution)
    }

    pub(in crate::producer) fn begin_materialization(
        &mut self,
        execution: BatchExecutionId,
    ) -> Result<(MaterializationAttempt, BatchPlan), ProducerStoreError> {
        let batch = self
            .batches
            .get_mut(&execution.batch_id())
            .ok_or(ProducerStoreError::UnknownBatch)?;
        batch.begin(execution)?;
        if batch.members.is_empty() {
            if !batch.abort(execution) {
                return Err(ProducerStoreError::StaleBatchExecution);
            }
            return Err(ProducerStoreError::EmptyBatch);
        }
        Ok((
            MaterializationAttempt { execution },
            BatchPlan {
                route: batch.route,
                members: batch.members.clone(),
            },
        ))
    }

    pub(in crate::producer) fn commit_materialization(
        &mut self,
        attempt: MaterializationAttempt,
    ) -> Result<(), ProducerStoreError> {
        let execution = attempt.into_execution();
        let batch = self
            .batches
            .get_mut(&execution.batch_id())
            .ok_or(ProducerStoreError::UnknownBatch)?;
        batch.commit(execution)
    }

    pub(in crate::producer) fn abort_materialization(
        &mut self,
        attempt: MaterializationAttempt,
    ) -> MaterializationAbort {
        let execution = attempt.into_execution();
        if self
            .batches
            .get_mut(&execution.batch_id())
            .is_some_and(|batch| batch.abort(execution))
        {
            MaterializationAbort::Restored
        } else {
            MaterializationAbort::Superseded
        }
    }

    pub(in crate::producer) fn execution_route(
        &self,
        execution: BatchExecutionId,
    ) -> Result<super::BatchRoute, ProducerStoreError> {
        let batch = self
            .batches
            .get(&execution.batch_id())
            .ok_or(ProducerStoreError::UnknownBatch)?;
        match batch.state {
            BatchState::Materialized(current) if current == execution => Ok(batch.route),
            BatchState::ReadyForMaterialization(_)
            | BatchState::Materializing(_)
            | BatchState::Materialized(_)
            | BatchState::Submitted(_) => Err(ProducerStoreError::StaleBatchExecution),
            BatchState::Open => Err(ProducerStoreError::BatchAlreadyMaterialized),
        }
    }

    pub(in crate::producer) fn execution(
        &self,
        batch_id: kafka_client_core::BatchId,
    ) -> Result<Option<BatchExecutionId>, ProducerStoreError> {
        self.batches
            .get(&batch_id)
            .map(BatchAccumulator::execution)
            .ok_or(ProducerStoreError::UnknownBatch)
    }

    #[cfg(test)]
    pub(in crate::producer) fn replace_ready_for_test(
        &mut self,
        batch_id: kafka_client_core::BatchId,
        replacement: BatchExecutionId,
    ) {
        if let Some(batch) = self.batches.get_mut(&batch_id) {
            batch.state = BatchState::ReadyForMaterialization(replacement);
        }
    }
}
