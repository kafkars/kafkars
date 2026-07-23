//! Virtual accumulator membership and explicit execution-phase mechanics.

use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, ByteCount, OperationId, PayloadId,
};

use super::VirtualProducerState;
use crate::SimulationError;

#[derive(Debug, Default)]
pub(super) struct VirtualBatch {
    pub(super) members: Vec<OperationId>,
    pub(super) phase: VirtualBatchPhase,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum VirtualBatchPhase {
    #[default]
    Open,
    Ready(BatchExecutionId),
    Materializing(BatchExecutionId),
    Materialized(BatchExecutionId),
    AwaitingDriver(BatchExecutionId),
    Submitted(BatchExecutionId),
}

impl VirtualBatch {
    pub(super) fn contains(&self, operation_id: OperationId) -> bool {
        self.members.contains(&operation_id)
    }

    fn ensure_open(&self, batch_id: BatchId) -> Result<(), SimulationError> {
        if self.phase == VirtualBatchPhase::Open {
            Ok(())
        } else {
            Err(SimulationError::BatchMembershipClosed(batch_id))
        }
    }

    fn accumulate(
        &mut self,
        batch_id: BatchId,
        operation_id: OperationId,
    ) -> Result<(), SimulationError> {
        self.ensure_open(batch_id)?;
        self.members.push(operation_id);
        Ok(())
    }

    fn remove(
        &mut self,
        batch_id: BatchId,
        operation_id: OperationId,
    ) -> Result<(), SimulationError> {
        self.ensure_open(batch_id)?;
        let position = self
            .members
            .iter()
            .position(|member| *member == operation_id)
            .ok_or(SimulationError::OperationNotInBatch(operation_id))?;
        self.members.remove(position);
        Ok(())
    }

    fn materialize(&mut self, execution: BatchExecutionId) -> Result<(), SimulationError> {
        match self.phase {
            VirtualBatchPhase::Open => {
                let expected = BatchExecutionId::new(
                    execution.batch_id(),
                    BatchExecutionGeneration::initial(),
                );
                if execution != expected {
                    return Err(SimulationError::BatchExecutionMismatch {
                        expected: Some(expected),
                        actual: execution,
                    });
                }
                self.phase = VirtualBatchPhase::Ready(execution);
            }
            VirtualBatchPhase::Ready(current) if current == execution => {}
            VirtualBatchPhase::Ready(current)
            | VirtualBatchPhase::Materializing(current)
            | VirtualBatchPhase::Materialized(current)
            | VirtualBatchPhase::AwaitingDriver(current)
            | VirtualBatchPhase::Submitted(current) => {
                return Err(SimulationError::BatchExecutionMismatch {
                    expected: Some(current),
                    actual: execution,
                });
            }
        }
        self.phase = VirtualBatchPhase::Materializing(execution);
        self.phase = VirtualBatchPhase::Materialized(execution);
        Ok(())
    }

    fn submit(
        &mut self,
        execution: BatchExecutionId,
        deadline_operation_id: OperationId,
    ) -> Result<Vec<OperationId>, SimulationError> {
        match self.phase {
            VirtualBatchPhase::Materialized(current) if current == execution => {}
            VirtualBatchPhase::Materialized(current)
            | VirtualBatchPhase::Ready(current)
            | VirtualBatchPhase::Materializing(current)
            | VirtualBatchPhase::AwaitingDriver(current)
            | VirtualBatchPhase::Submitted(current)
                if current != execution =>
            {
                return Err(SimulationError::BatchExecutionMismatch {
                    expected: Some(current),
                    actual: execution,
                });
            }
            VirtualBatchPhase::Submitted(_) => {
                return Err(SimulationError::DuplicateBatchExecution(execution));
            }
            VirtualBatchPhase::Open
            | VirtualBatchPhase::Ready(_)
            | VirtualBatchPhase::Materializing(_)
            | VirtualBatchPhase::AwaitingDriver(_)
            | VirtualBatchPhase::Materialized(_) => {
                return Err(SimulationError::BatchNotMaterialized(execution));
            }
        }
        if !self.contains(deadline_operation_id) {
            return Err(SimulationError::OperationNotInBatch(deadline_operation_id));
        }
        self.phase = VirtualBatchPhase::AwaitingDriver(execution);
        Ok(self.members.clone())
    }
}

impl VirtualProducerState {
    pub(crate) fn submission_count(&self) -> usize {
        self.submissions.len()
    }

    #[cfg(test)]
    pub(crate) fn submitted_members(&self, execution: BatchExecutionId) -> Option<&[OperationId]> {
        self.submissions.get(&execution).map(Vec::as_slice)
    }

    pub(super) fn accumulate(
        &mut self,
        batch_id: BatchId,
        operation_id: OperationId,
        payload_id: PayloadId,
        expected: ByteCount,
    ) -> Result<(), SimulationError> {
        let actual = self
            .payloads
            .get(&payload_id)
            .copied()
            .ok_or(SimulationError::UnknownPayload(payload_id))?;
        if actual != expected {
            return Err(SimulationError::PayloadSizeMismatch { actual, expected });
        }
        if self.operation_payloads.contains_key(&operation_id) {
            return Err(SimulationError::DuplicateOperation(operation_id));
        }
        let batch = self.batches.entry(batch_id).or_default();
        batch.ensure_open(batch_id)?;
        batch.accumulate(batch_id, operation_id)?;
        self.operation_payloads.insert(operation_id, payload_id);
        Ok(())
    }

    pub(super) fn require_batch(&self, batch_id: BatchId) -> Result<(), SimulationError> {
        self.batches
            .contains_key(&batch_id)
            .then_some(())
            .ok_or(SimulationError::UnknownBatch(batch_id))
    }

    pub(super) fn remove_batch_member(
        &mut self,
        batch_id: BatchId,
        operation_id: OperationId,
    ) -> Result<(), SimulationError> {
        self.batches
            .get_mut(&batch_id)
            .ok_or(SimulationError::UnknownBatch(batch_id))?
            .remove(batch_id, operation_id)
    }

    pub(super) fn materialize(
        &mut self,
        execution: BatchExecutionId,
    ) -> Result<(), SimulationError> {
        self.batches
            .get_mut(&execution.batch_id())
            .ok_or(SimulationError::UnknownBatch(execution.batch_id()))?
            .materialize(execution)
    }

    pub(super) fn submit(
        &mut self,
        execution: BatchExecutionId,
        deadline_operation_id: OperationId,
    ) -> Result<(), SimulationError> {
        let history = match self.submissions.entry(execution) {
            std::collections::btree_map::Entry::Occupied(_) => {
                return Err(SimulationError::DuplicateBatchExecution(execution));
            }
            std::collections::btree_map::Entry::Vacant(slot) => slot,
        };
        let members = self
            .batches
            .get_mut(&execution.batch_id())
            .ok_or(SimulationError::UnknownBatch(execution.batch_id()))?
            .submit(execution, deadline_operation_id)?;
        history.insert(members);
        Ok(())
    }

    pub(super) fn release_batch(&mut self, batch_id: BatchId) -> Result<(), SimulationError> {
        self.batches
            .remove(&batch_id)
            .ok_or(SimulationError::UnknownBatch(batch_id))?;
        self.timers.remove(&batch_id);
        Ok(())
    }
}
