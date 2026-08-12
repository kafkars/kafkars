//! Sole owner of ordered operation-to-payload batch membership.

mod driver;
#[cfg(test)]
mod driver_test;
mod execution;
#[cfg(test)]
mod execution_test;
mod retry;
#[cfg(test)]
mod retry_test;
mod revision;
#[cfg(test)]
mod revision_test;

use kafka_client_core::{
    BatchExecutionId, BatchId, OperationId, PartitionIndex, PayloadId, TopicId,
};

use crate::id_hash::{IdMap, id_map};

use super::ProducerStoreError;
pub(in crate::producer) use driver::DriverAcceptancePlan;
pub(in crate::producer) use execution::{MaterializationAbort, MaterializationAttempt};
pub(in crate::producer) use revision::{
    BatchCancellationPhase, BatchRevisionExpectation, BatchRevisionReplacement,
    EngineBatchRevisionPlan,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BatchRoute {
    pub(super) topic_id: TopicId,
    pub(super) partition: PartitionIndex,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BatchMember {
    pub(super) operation_id: OperationId,
    pub(super) payload_id: PayloadId,
    pub(super) sticky_unkeyed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BatchState {
    Open,
    ReadyForMaterialization(BatchExecutionId),
    Materializing(BatchExecutionId),
    Materialized(BatchExecutionId),
    Submitted(BatchExecutionId),
    RetryWaiting(BatchExecutionId),
}

#[derive(Debug)]
struct BatchAccumulator {
    route: BatchRoute,
    state: BatchState,
    members: Vec<BatchMember>,
    sticky_unkeyed_members: usize,
}

impl BatchAccumulator {
    fn remove(&mut self, index: usize) -> BatchMember {
        let member = self.members.remove(index);
        if member.sticky_unkeyed {
            debug_assert!(self.sticky_unkeyed_members > 0);
            self.sticky_unkeyed_members = self.sticky_unkeyed_members.saturating_sub(1);
        }
        member
    }
}

/// Pure materialization preflight, consumed by the store coordinator.
#[derive(Debug)]
pub(super) struct BatchPlan {
    pub(super) route: BatchRoute,
    pub(super) members: Vec<BatchMember>,
}

/// Bounded-by-record-count batch indexes and ordered membership.
#[derive(Debug, Default)]
pub(super) struct BatchStore {
    max_batches: usize,
    batches: IdMap<BatchId, BatchAccumulator>,
    operations: IdMap<OperationId, BatchId>,
    payloads: IdMap<PayloadId, BatchId>,
}

impl BatchStore {
    pub(super) const fn new(max_batches: usize) -> Self {
        Self {
            max_batches,
            batches: id_map(),
            operations: id_map(),
            payloads: id_map(),
        }
    }

    #[cfg(test)]
    pub(super) fn append(
        &mut self,
        batch_id: BatchId,
        operation_id: OperationId,
        payload_id: PayloadId,
        route: BatchRoute,
    ) -> Result<(), ProducerStoreError> {
        self.append_partitioned(batch_id, operation_id, payload_id, route, false)
    }

    pub(super) fn append_partitioned(
        &mut self,
        batch_id: BatchId,
        operation_id: OperationId,
        payload_id: PayloadId,
        route: BatchRoute,
        sticky_unkeyed: bool,
    ) -> Result<(), ProducerStoreError> {
        if self.operations.contains_key(&operation_id) {
            return Err(ProducerStoreError::DuplicateOperation);
        }
        if self.payloads.contains_key(&payload_id) {
            return Err(ProducerStoreError::DuplicatePayloadMembership);
        }
        if !self.batches.contains_key(&batch_id) && self.batches.len() >= self.max_batches {
            return Err(ProducerStoreError::BatchCapacity);
        }
        if let Some(batch) = self.batches.get(&batch_id) {
            if batch.state != BatchState::Open {
                return Err(ProducerStoreError::BatchAlreadyMaterialized);
            }
            if batch.route != route {
                return Err(ProducerStoreError::BatchRouteMismatch);
            }
        }
        let member = BatchMember {
            operation_id,
            payload_id,
            sticky_unkeyed,
        };
        let batch = self
            .batches
            .entry(batch_id)
            .or_insert_with(|| BatchAccumulator {
                route,
                state: BatchState::Open,
                members: Vec::new(),
                sticky_unkeyed_members: 0,
            });
        if sticky_unkeyed {
            batch.sticky_unkeyed_members = batch
                .sticky_unkeyed_members
                .checked_add(1)
                .ok_or(ProducerStoreError::RetainedSizeOverflow)?;
        }
        batch.members.push(member);
        self.operations.insert(operation_id, batch_id);
        self.payloads.insert(payload_id, batch_id);
        Ok(())
    }

    pub(super) fn remove_member(
        &mut self,
        batch_id: BatchId,
        operation_id: OperationId,
    ) -> Result<PayloadId, ProducerStoreError> {
        if self.operations.get(&operation_id) != Some(&batch_id) {
            return Err(ProducerStoreError::UnknownBatchMember);
        }
        let batch = self
            .batches
            .get_mut(&batch_id)
            .ok_or(ProducerStoreError::UnknownBatch)?;
        if batch.state != BatchState::Open {
            return Err(ProducerStoreError::BatchAlreadyMaterialized);
        }
        let Some(index) = batch
            .members
            .iter()
            .position(|member| member.operation_id == operation_id)
        else {
            return Err(ProducerStoreError::UnknownBatchMember);
        };
        let member = batch.remove(index);
        self.operations.remove(&operation_id);
        self.payloads.remove(&member.payload_id);
        if batch.members.is_empty() {
            self.batches.remove(&batch_id);
        }
        Ok(member.payload_id)
    }

    pub(super) fn release(
        &mut self,
        batch_id: BatchId,
    ) -> Result<Vec<BatchMember>, ProducerStoreError> {
        let Some(batch) = self.batches.remove(&batch_id) else {
            return Err(ProducerStoreError::UnknownBatch);
        };
        for member in &batch.members {
            self.operations.remove(&member.operation_id);
            self.payloads.remove(&member.payload_id);
        }
        Ok(batch.members)
    }

    pub(super) fn contains_payload(&self, payload_id: PayloadId) -> bool {
        self.payloads.contains_key(&payload_id)
    }

    pub(super) fn execution_contains_operation(
        &self,
        execution: kafka_client_core::BatchExecutionId,
        operation_id: OperationId,
    ) -> Result<bool, ProducerStoreError> {
        self.execution_route(execution)?;
        Ok(self.operations.get(&operation_id) == Some(&execution.batch_id()))
    }

    pub(super) fn len(&self) -> usize {
        self.batches.len()
    }
    #[cfg(test)]
    pub(super) fn record_count(&self, batch_id: BatchId) -> Result<u32, ProducerStoreError> {
        let count = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerStoreError::UnknownBatch)?
            .members
            .len();
        if count == 0 {
            return Err(ProducerStoreError::EmptyBatch);
        }
        u32::try_from(count).map_err(|_overflow| ProducerStoreError::BatchRecordCountOutOfRange)
    }

    pub(super) fn clear_terminal(&mut self) {
        self.batches.clear();
        self.operations.clear();
        self.payloads.clear();
    }
}
