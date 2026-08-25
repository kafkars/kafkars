//! Linear shared-handle views over canonically retained batch records.

use std::sync::Arc;

use kafka_client_core::{BatchExecutionId, ProducerIdentity, ProducerSequenceLease};

use super::ProducerStore;
use crate::producer::{
    ProducerRecord, ProducerStoreError,
    batch_store::{MaterializationAbort, MaterializationAttempt},
    materialization::MaterializationBatch,
};

impl ProducerStore {
    /// Begins one exact attempt and borrows shared handles from canonical records.
    pub(crate) fn materialization_view_idempotent(
        &mut self,
        execution: BatchExecutionId,
        max_batch_bytes: usize,
        identity: ProducerIdentity,
        sequence: ProducerSequenceLease,
    ) -> Result<(MaterializationAttempt, MaterializationBatch), ProducerStoreError> {
        if let Some(route) = self.batches.seal_for_materialization(execution)? {
            self.records
                .topics
                .partition_batch_sealed(route.topic_id, route.partition)?;
        }
        let (attempt, plan) = self.batches.begin_materialization(execution)?;
        let view = (|| {
            let partition = i32::try_from(plan.route.partition.get())
                .map_err(|_| ProducerStoreError::PartitionOutOfRange)?;
            let mut expected_topic: Option<Arc<str>> = None;
            let mut expected_topic_uuid = None;
            let mut validated_topic_generation: Option<
                kafka_client_core::partitioning::TopicMetadataGeneration,
            > = None;
            let mut leader_broker_id = None;
            for member in &plan.members {
                let record = self.records.record(member.payload_id)?;
                if self.records.route(member.payload_id)?.0 != plan.route.topic_id {
                    return Err(ProducerStoreError::BatchRouteMismatch);
                }
                match expected_topic.as_deref() {
                    Some(topic) if topic != record.topic().as_ref() => {
                        return Err(ProducerStoreError::BatchRouteMismatch);
                    }
                    None => expected_topic = Some(Arc::clone(record.topic())),
                    _ => {}
                }
                match (expected_topic_uuid, record.expected_topic_uuid()) {
                    (Some(expected), Some(candidate)) if expected != candidate => {
                        return Err(ProducerStoreError::TopicIdentityMismatch);
                    }
                    (None, Some(candidate)) => expected_topic_uuid = Some(candidate),
                    _ => {}
                }
                if record.expected_topic_uuid().is_some() {
                    let generation = record
                        .validated_topic_generation()
                        .ok_or(ProducerStoreError::InvalidPayloadState)?;
                    validated_topic_generation = Some(
                        validated_topic_generation
                            .map_or(generation, |current| current.max(generation)),
                    );
                }
                leader_broker_id = match leader_broker_id {
                    None => Some(record.leader_broker_id()),
                    Some(expected) if expected == record.leader_broker_id() => Some(expected),
                    Some(_) => Some(None),
                };
            }
            let topic = expected_topic.ok_or(ProducerStoreError::EmptyBatch)?;
            let source_retained_bytes =
                plan.members.iter().try_fold(0_usize, |retained, member| {
                    let member_bytes = self.records.retained_bytes(member.payload_id)?;
                    let member_bytes = usize::try_from(member_bytes.get())
                        .map_err(|_| ProducerStoreError::RetainedSizeOverflow)?;
                    retained
                        .checked_add(member_bytes)
                        .ok_or(ProducerStoreError::RetainedSizeOverflow)
                })?;
            let records = plan
                .members
                .iter()
                .map(|member| {
                    self.records
                        .record(member.payload_id)
                        .map(ProducerRecord::materialization_view)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(MaterializationBatch::idempotent(
                topic,
                partition,
                leader_broker_id.flatten(),
                records,
                max_batch_bytes,
                source_retained_bytes,
                identity,
                sequence,
            )
            .with_expected_topic_identity(expected_topic_uuid, validated_topic_generation))
        })();
        match view {
            Ok(batch) => Ok((attempt, batch)),
            Err(error) => match self.batches.abort_materialization(attempt) {
                MaterializationAbort::Restored => Err(error),
                MaterializationAbort::Superseded => Err(ProducerStoreError::StaleBatchExecution),
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn materialization_view(
        &mut self,
        execution: BatchExecutionId,
        max_batch_bytes: usize,
    ) -> Result<(MaterializationAttempt, MaterializationBatch), ProducerStoreError> {
        let identity =
            ProducerIdentity::try_new(1, 0).ok_or(ProducerStoreError::StaleBatchExecution)?;
        let record_count = self.batches.record_count(execution.batch_id())?;
        let sequence = ProducerSequenceLease::try_new(0, record_count)
            .ok_or(ProducerStoreError::StaleBatchExecution)?;
        self.materialization_view_idempotent(execution, max_batch_bytes, identity, sequence)
    }

    #[cfg(test)]
    pub(crate) fn sequence_for_test(
        &self,
        execution: BatchExecutionId,
    ) -> Result<ProducerSequenceLease, ProducerStoreError> {
        let record_count = self.batches.record_count(execution.batch_id())?;
        ProducerSequenceLease::try_new(0, record_count).ok_or(ProducerStoreError::EmptyBatch)
    }
}
