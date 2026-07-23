//! Sole owner of ordered operation-to-payload batch membership.

use std::collections::BTreeMap;

use kafka_client_core::{BatchId, OperationId, PartitionIndex, PayloadId, TopicId};

use super::ProducerStoreError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BatchRoute {
    pub(super) topic_id: TopicId,
    pub(super) partition: PartitionIndex,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BatchMember {
    pub(super) operation_id: OperationId,
    pub(super) payload_id: PayloadId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BatchState {
    Accumulating,
    Materializing,
    Materialized,
}

#[derive(Debug)]
struct BatchAccumulator {
    route: BatchRoute,
    state: BatchState,
    members: Vec<BatchMember>,
}

impl BatchAccumulator {
    fn push(&mut self, member: BatchMember) {
        self.members.push(member);
    }

    fn remove(&mut self, index: usize) -> BatchMember {
        self.members.remove(index)
    }

    fn begin_materialization(&mut self) {
        self.state = BatchState::Materializing;
    }

    fn finish_materialization(&mut self) {
        self.state = BatchState::Materialized;
    }

    fn cancel_materialization(&mut self) {
        self.state = BatchState::Accumulating;
    }
}

/// Pure materialization preflight, consumed by the store coordinator.
#[derive(Debug)]
pub(super) struct BatchPlan {
    pub(super) batch_id: BatchId,
    pub(super) route: BatchRoute,
    pub(super) members: Vec<BatchMember>,
}

/// Bounded-by-record-count batch indexes and ordered membership.
#[derive(Debug, Default)]
pub(super) struct BatchStore {
    max_batches: usize,
    batches: BTreeMap<BatchId, BatchAccumulator>,
    operations: BTreeMap<OperationId, BatchId>,
    payloads: BTreeMap<PayloadId, BatchId>,
}

impl BatchStore {
    pub(super) const fn new(max_batches: usize) -> Self {
        Self {
            max_batches,
            batches: BTreeMap::new(),
            operations: BTreeMap::new(),
            payloads: BTreeMap::new(),
        }
    }

    pub(super) fn append(
        &mut self,
        batch_id: BatchId,
        operation_id: OperationId,
        payload_id: PayloadId,
        route: BatchRoute,
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
            if batch.state != BatchState::Accumulating {
                return Err(ProducerStoreError::BatchAlreadyMaterialized);
            }
            if batch.route != route {
                return Err(ProducerStoreError::BatchRouteMismatch);
            }
        }
        let member = BatchMember {
            operation_id,
            payload_id,
        };
        self.batches
            .entry(batch_id)
            .or_insert_with(|| BatchAccumulator {
                route,
                state: BatchState::Accumulating,
                members: Vec::new(),
            })
            .push(member);
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
        if batch.state != BatchState::Accumulating {
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

    pub(super) fn plan(&self, batch_id: BatchId) -> Result<BatchPlan, ProducerStoreError> {
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerStoreError::UnknownBatch)?;
        if batch.state != BatchState::Accumulating {
            return Err(ProducerStoreError::BatchAlreadyMaterialized);
        }
        if batch.members.is_empty() {
            return Err(ProducerStoreError::EmptyBatch);
        }
        Ok(BatchPlan {
            batch_id,
            route: batch.route,
            members: batch.members.clone(),
        })
    }

    pub(super) fn begin_materialization(
        &mut self,
        batch_id: BatchId,
    ) -> Result<(), ProducerStoreError> {
        let batch = self
            .batches
            .get_mut(&batch_id)
            .ok_or(ProducerStoreError::UnknownBatch)?;
        if batch.state != BatchState::Accumulating {
            return Err(ProducerStoreError::BatchAlreadyMaterialized);
        }
        batch.begin_materialization();
        Ok(())
    }

    pub(super) fn finish_materialization(
        &mut self,
        batch_id: BatchId,
    ) -> Result<(), ProducerStoreError> {
        let batch = self
            .batches
            .get_mut(&batch_id)
            .ok_or(ProducerStoreError::UnknownBatch)?;
        if batch.state != BatchState::Materializing {
            return Err(ProducerStoreError::BatchAlreadyMaterialized);
        }
        batch.finish_materialization();
        Ok(())
    }

    pub(super) fn cancel_materialization(&mut self, batch_id: BatchId) {
        if let Some(batch) = self.batches.get_mut(&batch_id)
            && batch.state == BatchState::Materializing
        {
            batch.cancel_materialization();
        }
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

    pub(super) fn route(&self, batch_id: BatchId) -> Result<BatchRoute, ProducerStoreError> {
        self.batches
            .get(&batch_id)
            .map(|batch| batch.route)
            .ok_or(ProducerStoreError::UnknownBatch)
    }

    pub(super) fn len(&self) -> usize {
        self.batches.len()
    }
}
