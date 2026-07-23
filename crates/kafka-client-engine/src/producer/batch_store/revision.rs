//! Atomic replacement of one immutable sealed-batch membership snapshot.

use kafka_client_core::{BatchExecutionGeneration, BatchExecutionId, BatchId, OperationId};

use super::{BatchMember, BatchState, BatchStore};
use crate::producer::ProducerStoreError;

/// Engine batch phase required by the pending mechanism preflight.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::producer) enum BatchRevisionExpectation {
    OpenForMaterialization,
    ReadyForMaterialization,
    Materialized,
}

/// Exact engine mechanism phase retained for one cancellable operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::producer) enum BatchCancellationPhase {
    Open(BatchId),
    Sealed(BatchExecutionId),
    Submitted,
}

/// Core-facing replacement selected by one exact engine revision preflight.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::producer) enum BatchRevisionReplacement {
    Empty,
    Next(BatchExecutionId),
    Exhausted,
}

/// Linear proof that one exact batch revision can commit without failure.
#[derive(Debug)]
#[must_use = "a preflighted batch revision must be committed or abandoned"]
pub(in crate::producer) struct EngineBatchRevisionPlan {
    previous: BatchExecutionId,
    expected_replacement: BatchRevisionReplacement,
    removed_index: usize,
    removed: BatchMember,
}

impl BatchStore {
    pub(in crate::producer) fn cancellation_phase(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<BatchCancellationPhase>, ProducerStoreError> {
        let Some(batch_id) = self.operations.get(&operation_id).copied() else {
            return Ok(None);
        };
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerStoreError::UnknownBatch)?;
        Ok(Some(match batch.state {
            BatchState::Open => BatchCancellationPhase::Open(batch_id),
            BatchState::ReadyForMaterialization(execution)
            | BatchState::Materializing(execution)
            | BatchState::Materialized(execution) => BatchCancellationPhase::Sealed(execution),
            BatchState::Submitted(_) => BatchCancellationPhase::Submitted,
        }))
    }

    pub(in crate::producer) fn plan_revision(
        &self,
        previous: BatchExecutionId,
        removed_operation_id: OperationId,
        expectation: BatchRevisionExpectation,
    ) -> Result<EngineBatchRevisionPlan, ProducerStoreError> {
        let batch_id = previous.batch_id();
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerStoreError::UnknownBatch)?;
        let expected_state = match expectation {
            BatchRevisionExpectation::OpenForMaterialization => BatchState::Open,
            BatchRevisionExpectation::ReadyForMaterialization => {
                BatchState::ReadyForMaterialization(previous)
            }
            BatchRevisionExpectation::Materialized => BatchState::Materialized(previous),
        };
        if batch.state != expected_state {
            return Err(ProducerStoreError::StaleBatchExecution);
        }
        if self.operations.get(&removed_operation_id) != Some(&batch_id) {
            return Err(ProducerStoreError::UnknownBatchMember);
        }
        let removed_index = batch
            .members
            .iter()
            .position(|member| member.operation_id == removed_operation_id)
            .ok_or(ProducerStoreError::UnknownBatchMember)?;
        let removed = batch.members[removed_index];
        if self.payloads.get(&removed.payload_id) != Some(&batch_id) {
            return Err(ProducerStoreError::UnknownBatchMember);
        }
        let expected_replacement = expected_replacement(previous, batch.members.len());
        Ok(EngineBatchRevisionPlan {
            previous,
            expected_replacement,
            removed_index,
            removed,
        })
    }

    pub(in crate::producer) fn commit_revision(&mut self, plan: EngineBatchRevisionPlan) {
        let (previous, expected_replacement, removed_index, removed) = plan.into_parts();
        let batch_id = previous.batch_id();
        match expected_replacement {
            BatchRevisionReplacement::Next(replacement) => {
                if let Some(batch) = self.batches.get_mut(&batch_id) {
                    let committed = batch.remove(removed_index);
                    debug_assert_eq!(committed, removed);
                    batch.state = BatchState::ReadyForMaterialization(replacement);
                }
            }
            BatchRevisionReplacement::Empty => {
                let removed_batch = self.batches.remove(&batch_id);
                debug_assert!(removed_batch.is_some());
            }
            BatchRevisionReplacement::Exhausted => {
                debug_assert!(false, "exhausted revision plans cannot commit");
                return;
            }
        }
        let operation = self.operations.remove(&removed.operation_id);
        let payload = self.payloads.remove(&removed.payload_id);
        debug_assert_eq!(operation, Some(batch_id));
        debug_assert_eq!(payload, Some(batch_id));
    }
}

impl EngineBatchRevisionPlan {
    pub(in crate::producer) const fn expected_replacement(&self) -> BatchRevisionReplacement {
        self.expected_replacement
    }

    fn into_parts(
        self,
    ) -> (
        BatchExecutionId,
        BatchRevisionReplacement,
        usize,
        BatchMember,
    ) {
        (
            self.previous,
            self.expected_replacement,
            self.removed_index,
            self.removed,
        )
    }
}

fn expected_replacement(
    previous: BatchExecutionId,
    member_count: usize,
) -> BatchRevisionReplacement {
    if member_count == 1 {
        return BatchRevisionReplacement::Empty;
    }
    previous
        .generation()
        .get()
        .checked_add(1)
        .and_then(BatchExecutionGeneration::try_from_raw)
        .map_or(BatchRevisionReplacement::Exhausted, |generation| {
            BatchRevisionReplacement::Next(BatchExecutionId::new(previous.batch_id(), generation))
        })
}
