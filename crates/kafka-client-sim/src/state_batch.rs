//! Virtual accumulator membership and whole-batch release mechanics.

use kafka_client_core::{BatchId, ByteCount, OperationId, PayloadId};

use super::VirtualProducerState;
use crate::SimulationError;

impl VirtualProducerState {
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
        self.operation_payloads.insert(operation_id, payload_id);
        self.batches.entry(batch_id).or_default().push(operation_id);
        Ok(())
    }

    pub(super) fn require_batch(&self, batch_id: BatchId) -> Result<(), SimulationError> {
        self.batches
            .contains_key(&batch_id)
            .then_some(())
            .ok_or(SimulationError::UnknownBatch(batch_id))
    }

    pub(super) fn require_batch_member(
        &self,
        batch_id: BatchId,
        operation_id: OperationId,
    ) -> Result<(), SimulationError> {
        self.batches
            .get(&batch_id)
            .ok_or(SimulationError::UnknownBatch(batch_id))?
            .contains(&operation_id)
            .then_some(())
            .ok_or(SimulationError::OperationNotInBatch(operation_id))
    }

    pub(super) fn remove_batch_member(
        &mut self,
        batch_id: BatchId,
        operation_id: OperationId,
    ) -> Result<(), SimulationError> {
        let members = self
            .batches
            .get_mut(&batch_id)
            .ok_or(SimulationError::UnknownBatch(batch_id))?;
        let position = members
            .iter()
            .position(|member| *member == operation_id)
            .ok_or(SimulationError::OperationNotInBatch(operation_id))?;
        members.remove(position);
        Ok(())
    }

    pub(super) fn release_batch(&mut self, batch_id: BatchId) -> Result<(), SimulationError> {
        self.batches
            .remove(&batch_id)
            .ok_or(SimulationError::UnknownBatch(batch_id))?;
        self.timers.remove(&batch_id);
        self.materialized.remove(&batch_id);
        Ok(())
    }
}
