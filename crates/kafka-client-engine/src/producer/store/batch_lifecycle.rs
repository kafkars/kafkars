//! Ordered batch membership and exact execution-phase delegation.

use kafka_client_core::{
    BatchExecutionId, BatchId, ByteCount, OperationId, PartitionIndex, PayloadId, TopicId,
};

use super::ProducerStore;
use crate::producer::{
    ProducerStoreError,
    batch_store::{
        BatchCancellationPhase, BatchRevisionExpectation, BatchRoute, DriverAcceptancePlan,
        EngineBatchRevisionPlan, MaterializationAbort, MaterializationAttempt,
    },
};

impl ProducerStore {
    /// Appends a core-accepted operation to one ordered logical batch.
    pub(crate) fn accumulate(
        &mut self,
        batch_id: BatchId,
        operation_id: OperationId,
        payload_id: PayloadId,
    ) -> Result<ByteCount, ProducerStoreError> {
        let (topic_id, partition) = self.records.route(payload_id)?;
        let retained = self.records.retained_bytes(payload_id)?;
        self.batches.append(
            batch_id,
            operation_id,
            payload_id,
            BatchRoute {
                topic_id,
                partition,
            },
        )?;
        Ok(retained)
    }

    /// Removes one open-batch member while preserving all sibling order.
    pub(crate) fn remove_member(
        &mut self,
        batch_id: BatchId,
        operation_id: OperationId,
    ) -> Result<PayloadId, ProducerStoreError> {
        self.batches.remove_member(batch_id, operation_id)
    }

    /// Preflights one exact sealed-membership replacement without mutation.
    pub(in crate::producer) fn plan_batch_revision(
        &self,
        previous: BatchExecutionId,
        removed_operation_id: OperationId,
        expectation: BatchRevisionExpectation,
    ) -> Result<EngineBatchRevisionPlan, ProducerStoreError> {
        self.batches
            .plan_revision(previous, removed_operation_id, expectation)
    }

    /// Commits a previously validated sealed-membership replacement.
    pub(in crate::producer) fn commit_batch_revision(&mut self, plan: EngineBatchRevisionPlan) {
        self.batches.commit_revision(plan);
    }

    /// Retains canonical membership under a fresh core-authorized retry generation.
    pub(in crate::producer) fn start_batch_retry(
        &mut self,
        previous: BatchExecutionId,
        replacement: BatchExecutionId,
    ) -> Result<(), ProducerStoreError> {
        self.batches.start_retry(previous, replacement)
    }

    /// Makes a retry-wait execution eligible for the declared materialization effect.
    pub(in crate::producer) fn activate_batch_retry(
        &mut self,
        execution: BatchExecutionId,
    ) -> Result<(), ProducerStoreError> {
        self.batches.activate_retry(execution)
    }

    /// Returns the exact engine mechanism phase for cancellation preflight.
    pub(in crate::producer) fn cancellation_phase(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<BatchCancellationPhase>, ProducerStoreError> {
        self.batches.cancellation_phase(operation_id)
    }

    /// Preflights the engine's materialized-to-submitted phase transition.
    pub(in crate::producer) fn plan_driver_accepted(
        &self,
        execution: BatchExecutionId,
    ) -> Result<DriverAcceptancePlan, ProducerStoreError> {
        self.batches.plan_driver_accepted(execution)
    }

    /// Commits driver ownership after core accepted the same exact fact.
    pub(in crate::producer) fn commit_driver_accepted(&mut self, plan: DriverAcceptancePlan) {
        self.batches.commit_driver_accepted(plan);
    }

    /// Releases logical batch membership exactly once before payload release.
    pub(crate) fn release_batch(&mut self, batch_id: BatchId) -> Result<(), ProducerStoreError> {
        let _members = self.batches.release(batch_id)?;
        Ok(())
    }

    /// Returns route provenance only for the exact sealed membership snapshot.
    pub(crate) fn execution_route(
        &self,
        execution: BatchExecutionId,
    ) -> Result<(TopicId, PartitionIndex), ProducerStoreError> {
        let route = self.batches.execution_route(execution)?;
        Ok((route.topic_id, route.partition))
    }

    /// Proves that one operation belongs to the exact materialized execution.
    pub(crate) fn execution_contains_operation(
        &self,
        execution: BatchExecutionId,
        operation_id: OperationId,
    ) -> Result<bool, ProducerStoreError> {
        self.batches
            .execution_contains_operation(execution, operation_id)
    }

    /// Commits an exact attempt only after encoded bytes are retained.
    pub(crate) fn commit_materialization(
        &mut self,
        attempt: MaterializationAttempt,
    ) -> Result<(), ProducerStoreError> {
        self.batches.commit_materialization(attempt)
    }

    /// Returns one failed exact attempt to its immutable ready phase.
    pub(crate) fn abort_materialization(
        &mut self,
        attempt: MaterializationAttempt,
    ) -> MaterializationAbort {
        self.batches.abort_materialization(attempt)
    }

    /// Returns the execution retained by a sealed batch for cleanup preflight.
    pub(crate) fn batch_execution(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<BatchExecutionId>, ProducerStoreError> {
        self.batches.execution(batch_id)
    }

    #[cfg(test)]
    pub(crate) fn replace_batch_execution_for_test(
        &mut self,
        batch_id: BatchId,
        replacement: BatchExecutionId,
    ) {
        self.batches.replace_ready_for_test(batch_id, replacement);
    }
}
