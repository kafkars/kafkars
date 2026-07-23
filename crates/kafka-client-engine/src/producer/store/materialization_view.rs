//! Linear shared-handle views over canonically retained batch records.

use std::sync::Arc;

use kafka_client_core::BatchExecutionId;

use super::ProducerStore;
use crate::producer::{
    ProducerRecord, ProducerStoreError,
    batch_store::{MaterializationAbort, MaterializationAttempt},
    materialization::MaterializationBatch,
};

impl ProducerStore {
    /// Begins one exact attempt and borrows shared handles from canonical records.
    pub(crate) fn materialization_view(
        &mut self,
        execution: BatchExecutionId,
        max_batch_bytes: usize,
    ) -> Result<(MaterializationAttempt, MaterializationBatch), ProducerStoreError> {
        self.batches.seal_for_materialization(execution)?;
        let (attempt, plan) = self.batches.begin_materialization(execution)?;
        let view = (|| {
            let partition = i32::try_from(plan.route.partition.get())
                .map_err(|_| ProducerStoreError::PartitionOutOfRange)?;
            let mut expected_topic: Option<Arc<str>> = None;
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
            }
            let topic = expected_topic.ok_or(ProducerStoreError::EmptyBatch)?;
            let records = plan
                .members
                .iter()
                .map(|member| {
                    self.records
                        .record(member.payload_id)
                        .map(ProducerRecord::materialization_view)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(MaterializationBatch::new(
                topic,
                partition,
                records,
                max_batch_bytes,
            ))
        })();
        match view {
            Ok(batch) => Ok((attempt, batch)),
            Err(error) => match self.batches.abort_materialization(attempt) {
                MaterializationAbort::Restored => Err(error),
                MaterializationAbort::Superseded => Err(ProducerStoreError::StaleBatchExecution),
            },
        }
    }
}
