//! Virtual engine ownership for payloads, accumulators, timers, and results.

mod batch;
mod batch_revision;
#[cfg(test)]
mod batch_revision_test;
#[cfg(test)]
mod batch_test;
mod flush;
mod retry;
#[cfg(test)]
mod retry_test;
mod terminal;

use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};

use kafka_client_core::{
    BatchExecutionId, BatchId, BatchTimerGeneration, ByteCount, Deadline, OperationId, PayloadId,
    ProducerCompletion, ProducerEffect,
};

use crate::SimulationError;
use batch::VirtualBatch;

#[derive(Debug, Default)]
pub(crate) struct VirtualProducerState {
    payloads: BTreeMap<PayloadId, ByteCount>,
    operation_payloads: BTreeMap<OperationId, PayloadId>,
    batches: BTreeMap<BatchId, VirtualBatch>,
    submissions: BTreeMap<BatchExecutionId, Vec<OperationId>>,
    timers: BTreeMap<BatchId, (BatchTimerGeneration, Deadline)>,
    terminals: BTreeMap<OperationId, ProducerCompletion>,
    released_terminals: BTreeSet<OperationId>,
    flushes: flush::VirtualFlushes,
    trace: Vec<ProducerEffect>,
}

impl VirtualProducerState {
    pub(crate) fn new(flush_capacity: usize) -> Self {
        Self {
            flushes: flush::VirtualFlushes::new(flush_capacity),
            ..Self::default()
        }
    }

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
            ProducerEffect::AcquireProducerIdentity { .. } => {}
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
            ProducerEffect::MaterializeBatch { execution, .. } => {
                self.materialize(execution)?;
            }
            ProducerEffect::ReviseBatchExecution {
                previous,
                replacement,
                removed_operation_id,
            } => {
                self.revise_batch_execution(previous, replacement, removed_operation_id)?;
            }
            ProducerEffect::RetryBatchExecution {
                previous,
                replacement,
            } => {
                self.retry_batch_execution(previous, replacement)?;
            }
            ProducerEffect::SubmitProduce {
                execution,
                deadline_operation_id,
                ..
            } => {
                self.submit(execution, deadline_operation_id)?;
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
            ProducerEffect::AcceptFlush { flush_id, barrier } => {
                self.accept_flush(flush_id, barrier)?
            }
            ProducerEffect::CompleteFlush { flush_id } => self.complete_flush(flush_id)?,
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

    pub(crate) fn contains_payload(&self, payload_id: PayloadId) -> bool {
        self.payloads.contains_key(&payload_id)
    }

    pub(crate) fn contains_batch(&self, batch_id: BatchId) -> bool {
        self.batches.contains_key(&batch_id)
    }

    pub(crate) fn trace(&self) -> &[ProducerEffect] {
        &self.trace
    }
}
