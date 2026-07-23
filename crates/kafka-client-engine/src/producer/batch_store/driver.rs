//! Exact engine phase transition after driver ownership commits.

use kafka_client_core::BatchExecutionId;

use super::{BatchState, BatchStore};
use crate::producer::ProducerStoreError;

/// Linear proof that one materialized execution can become submitted.
#[derive(Debug)]
#[must_use = "a preflighted driver acceptance must be committed or abandoned"]
pub(in crate::producer) struct DriverAcceptancePlan {
    execution: BatchExecutionId,
}

impl DriverAcceptancePlan {
    const fn into_execution(self) -> BatchExecutionId {
        self.execution
    }
}

impl BatchStore {
    pub(in crate::producer) fn plan_driver_accepted(
        &self,
        execution: BatchExecutionId,
    ) -> Result<DriverAcceptancePlan, ProducerStoreError> {
        let batch = self
            .batches
            .get(&execution.batch_id())
            .ok_or(ProducerStoreError::UnknownBatch)?;
        if batch.state != BatchState::Materialized(execution) {
            return Err(ProducerStoreError::StaleBatchExecution);
        }
        Ok(DriverAcceptancePlan { execution })
    }

    pub(in crate::producer) fn commit_driver_accepted(&mut self, plan: DriverAcceptancePlan) {
        let execution = plan.into_execution();
        if let Some(batch) = self.batches.get_mut(&execution.batch_id()) {
            debug_assert_eq!(batch.state, BatchState::Materialized(execution));
            batch.state = BatchState::Submitted(execution);
        }
    }
}
