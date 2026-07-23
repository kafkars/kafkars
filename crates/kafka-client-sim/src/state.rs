//! Virtual engine ownership for payloads, accumulators, timers, and results.

#[path = "state_batch.rs"]
mod batch;

use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};

use kafka_client_core::{
    BatchId, BatchTimerGeneration, ByteCount, Deadline, OperationId, PayloadId, ProducerCompletion,
    ProducerEffect,
};

use crate::SimulationError;

#[derive(Debug, Default)]
pub(crate) struct VirtualProducerState {
    payloads: BTreeMap<PayloadId, ByteCount>,
    operation_payloads: BTreeMap<OperationId, PayloadId>,
    batches: BTreeMap<BatchId, Vec<OperationId>>,
    timers: BTreeMap<BatchId, (BatchTimerGeneration, Deadline)>,
    materialized: BTreeSet<BatchId>,
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

    pub(crate) fn interpret(&mut self, effect: ProducerEffect) -> Result<(), SimulationError> {
        match effect {
            ProducerEffect::AccumulateExplicit {
                operation_id,
                batch_id,
                record,
                ..
            } => self.accumulate(
                batch_id,
                operation_id,
                record.payload_id(),
                record.retained_bytes(),
            )?,
            ProducerEffect::ArmBatchTimer {
                batch_id,
                generation,
                deadline,
            } => {
                self.require_batch(batch_id)?;
                self.timers.insert(batch_id, (generation, deadline));
            }
            ProducerEffect::CancelBatchTimer {
                batch_id,
                generation,
            } => {
                self.require_batch(batch_id)?;
                if self
                    .timers
                    .get(&batch_id)
                    .is_some_and(|(actual, _)| *actual == generation)
                {
                    self.timers.remove(&batch_id);
                }
            }
            ProducerEffect::MaterializeBatch { batch_id, .. } => {
                self.require_batch(batch_id)?;
                self.materialized.insert(batch_id);
            }
            ProducerEffect::SubmitProduce {
                batch_id,
                deadline_operation_id,
                ..
            } => {
                self.require_batch(batch_id)?;
                if !self.materialized.contains(&batch_id) {
                    return Err(SimulationError::BatchNotMaterialized(batch_id));
                }
                self.require_batch_member(batch_id, deadline_operation_id)?;
                self.submission_count += 1;
            }
            ProducerEffect::RemoveBatchMember {
                batch_id,
                operation_id,
            } => self.remove_batch_member(batch_id, operation_id)?,
            ProducerEffect::ReleaseBatch { batch_id } => self.release_batch(batch_id)?,
            ProducerEffect::ReleasePayload {
                payload_id,
                retained_bytes,
            } => self.release_payload(payload_id, retained_bytes)?,
            ProducerEffect::Complete {
                operation_id,
                completion,
            } => self.complete(operation_id, completion)?,
            ProducerEffect::AcceptFlush { .. } | ProducerEffect::CompleteFlush { .. } => {
                return Err(SimulationError::FlushControlUnavailable);
            }
        }
        self.trace.push(effect);
        Ok(())
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
        let batch_retained = self
            .batches
            .values()
            .any(|members| members.contains(&operation_id));
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

    pub(crate) fn take_timer_before(
        &mut self,
        target: kafka_client_core::Moment,
    ) -> Option<(BatchId, BatchTimerGeneration, Deadline)> {
        let batch_id = self
            .timers
            .iter()
            .filter(|(_, (_, deadline))| deadline.is_elapsed_at(target))
            .min_by_key(|(batch_id, (_, deadline))| (deadline.tick(), batch_id.get()))
            .map(|(batch_id, _)| *batch_id)?;
        self.timers
            .remove(&batch_id)
            .map(|(generation, deadline)| (batch_id, generation, deadline))
    }

    pub(crate) fn require_released_terminal(
        &self,
        operation_id: OperationId,
    ) -> Result<(), SimulationError> {
        self.released_terminals
            .contains(&operation_id)
            .then_some(())
            .ok_or(SimulationError::TerminalStillRetained(operation_id))
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
