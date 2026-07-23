//! Linear shared-handle views over canonically retained batch records.

use std::sync::Arc;

use kafka_client_core::BatchId;

use super::ProducerStore;
use crate::producer::{ProducerRecord, ProducerStoreError, materialization::MaterializationBatch};

impl ProducerStore {
    /// Clones only shared byte and topic handles in membership order.
    pub(crate) fn materialization_view(
        &mut self,
        batch_id: BatchId,
        max_batch_bytes: usize,
    ) -> Result<MaterializationBatch, ProducerStoreError> {
        let plan = self.batches.plan(batch_id)?;
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
        self.batches.begin_materialization(plan.batch_id)?;
        if let Err(error) = self.batches.finish_materialization(plan.batch_id) {
            self.batches.cancel_materialization(plan.batch_id);
            return Err(error);
        }
        Ok(MaterializationBatch::new(
            topic,
            partition,
            records,
            max_batch_bytes,
        ))
    }
}
