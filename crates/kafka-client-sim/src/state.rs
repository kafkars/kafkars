//! Virtual engine ownership for payloads, batches, submissions, and results.

use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};

use kafka_client_core::{
    BatchId, ByteCount, OperationId, PayloadId, ProducerCompletion, ProducerEffect,
};

use crate::SimulationError;

#[derive(Debug, Default)]
pub(crate) struct VirtualProducerState {
    payloads: BTreeMap<PayloadId, ByteCount>,
    operation_payloads: BTreeMap<OperationId, PayloadId>,
    batches: BTreeMap<BatchId, OperationId>,
    terminals: BTreeMap<OperationId, ProducerCompletion>,
    released_terminals: BTreeSet<OperationId>,
    submission_count: usize,
    trace: Vec<ProducerEffect>,
}

impl VirtualProducerState {
    pub(crate) fn retain_payload(
        &mut self,
        payload_id: PayloadId,
        bytes: ByteCount,
    ) -> Result<(), SimulationError> {
        match self.payloads.entry(payload_id) {
            Entry::Occupied(_) => Err(SimulationError::DuplicatePayload(payload_id)),
            Entry::Vacant(slot) => {
                slot.insert(bytes);
                Ok(())
            }
        }
    }

    pub(crate) fn materialize_batch(
        &mut self,
        batch_id: BatchId,
        operation_id: OperationId,
    ) -> Result<(), SimulationError> {
        match self.batches.entry(batch_id) {
            Entry::Occupied(_) => Err(SimulationError::DuplicateBatch(batch_id)),
            Entry::Vacant(slot) => {
                slot.insert(operation_id);
                Ok(())
            }
        }
    }

    pub(crate) fn interpret(&mut self, effect: ProducerEffect) -> Result<(), SimulationError> {
        match effect {
            ProducerEffect::AccumulateExplicit {
                operation_id,
                record,
                ..
            } => self.accumulate(operation_id, record.payload_id(), record.retained_bytes())?,
            ProducerEffect::SubmitProduce {
                operation_id,
                batch_id,
                ..
            } => {
                self.require_batch(batch_id, operation_id)?;
                self.submission_count += 1;
            }
            ProducerEffect::ReleaseBatch { batch_id } => {
                self.batches
                    .remove(&batch_id)
                    .ok_or(SimulationError::UnknownBatch(batch_id))?;
            }
            ProducerEffect::ReleasePayload {
                payload_id,
                retained_bytes,
            } => self.release_payload(payload_id, retained_bytes)?,
            ProducerEffect::Complete {
                operation_id,
                completion,
            } => self.complete(operation_id, completion)?,
        }
        self.trace.push(effect);
        Ok(())
    }

    fn accumulate(
        &mut self,
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
        self.operation_payloads.insert(operation_id, payload_id);
        Ok(())
    }

    fn require_batch(
        &self,
        batch_id: BatchId,
        expected: OperationId,
    ) -> Result<(), SimulationError> {
        let actual = self
            .batches
            .get(&batch_id)
            .copied()
            .ok_or(SimulationError::UnknownBatch(batch_id))?;
        if actual == expected {
            Ok(())
        } else {
            Err(SimulationError::BatchOperationMismatch { actual, expected })
        }
    }

    fn release_payload(
        &mut self,
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
        self.payloads.remove(&payload_id);
        Ok(())
    }

    fn complete(
        &mut self,
        operation_id: OperationId,
        completion: ProducerCompletion,
    ) -> Result<(), SimulationError> {
        let payload_retained = self
            .operation_payloads
            .get(&operation_id)
            .is_some_and(|payload_id| self.payloads.contains_key(payload_id));
        let batch_retained = self.batches.values().any(|owner| *owner == operation_id);
        if payload_retained || batch_retained {
            return Err(SimulationError::ResourceStillRetained(operation_id));
        }
        self.operation_payloads.remove(&operation_id);
        match self.terminals.entry(operation_id) {
            Entry::Occupied(_) => Err(SimulationError::DuplicateTerminal(operation_id)),
            Entry::Vacant(slot) => {
                slot.insert(completion);
                Ok(())
            }
        }
    }

    pub(crate) fn release_terminal(
        &mut self,
        operation_id: OperationId,
    ) -> Result<ProducerCompletion, SimulationError> {
        let completion = self
            .terminals
            .remove(&operation_id)
            .ok_or(SimulationError::UnknownTerminal(operation_id))?;
        self.released_terminals.insert(operation_id);
        Ok(completion)
    }

    pub(crate) fn require_released_terminal(
        &self,
        operation_id: OperationId,
    ) -> Result<(), SimulationError> {
        if self.released_terminals.contains(&operation_id) {
            Ok(())
        } else {
            Err(SimulationError::TerminalStillRetained(operation_id))
        }
    }

    pub(crate) fn finish_reclaim(&mut self, operation_id: OperationId) {
        self.released_terminals.remove(&operation_id);
    }

    pub(crate) fn contains_payload(&self, payload_id: PayloadId) -> bool {
        self.payloads.contains_key(&payload_id)
    }

    pub(crate) fn contains_batch(&self, batch_id: BatchId) -> bool {
        self.batches.contains_key(&batch_id)
    }

    pub(crate) fn terminal(&self, operation_id: OperationId) -> Option<ProducerCompletion> {
        self.terminals.get(&operation_id).copied()
    }

    pub(crate) const fn submission_count(&self) -> usize {
        self.submission_count
    }

    pub(crate) fn trace(&self) -> &[ProducerEffect] {
        &self.trace
    }
}
