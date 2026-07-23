//! Atomic transfer of a sealed accumulator into protocol-owned input fields.

use kafka_client_core::BatchId;

use super::ProducerStore;
use crate::producer::{
    MaterializationBatch, MaterializationHeader, MaterializationRecord, ProducerRecord,
    ProducerStoreError,
};

impl ProducerStore {
    /// Moves record fields in membership order without reading clocks or encoding.
    pub(crate) fn take_materialization(
        &mut self,
        batch_id: BatchId,
        max_batch_bytes: usize,
    ) -> Result<MaterializationBatch, ProducerStoreError> {
        let plan = self.batches.plan(batch_id)?;
        let partition = i32::try_from(plan.route.partition.get())
            .map_err(|_| ProducerStoreError::PartitionOutOfRange)?;
        let mut expected_topic: Option<&str> = None;
        for member in &plan.members {
            let record = self.records.record(member.payload_id)?;
            if self.records.route(member.payload_id)?.0 != plan.route.topic_id {
                return Err(ProducerStoreError::BatchRouteMismatch);
            }
            match expected_topic {
                Some(topic) if topic != record.topic().as_ref() => {
                    return Err(ProducerStoreError::BatchRouteMismatch);
                }
                None => expected_topic = Some(record.topic().as_ref()),
                _ => {}
            }
        }
        let topic = expected_topic
            .ok_or(ProducerStoreError::EmptyBatch)?
            .to_owned();
        self.batches.begin_materialization(plan.batch_id)?;
        let mut taken = Vec::with_capacity(plan.members.len());
        for member in &plan.members {
            match self.records.take_for_materialization(member.payload_id) {
                Ok(record) => taken.push((member.payload_id, record)),
                Err(error) => {
                    restore(&mut self.records, taken);
                    self.batches.cancel_materialization(plan.batch_id);
                    return Err(error);
                }
            }
        }
        if let Err(error) = self.batches.finish_materialization(plan.batch_id) {
            restore(&mut self.records, taken);
            self.batches.cancel_materialization(plan.batch_id);
            return Err(error);
        }
        let records = taken
            .into_iter()
            .map(|(_, record)| materialization_record(record))
            .collect();
        Ok(MaterializationBatch::new(
            topic,
            partition,
            records,
            max_batch_bytes,
        ))
    }
}

fn materialization_record(record: ProducerRecord) -> MaterializationRecord {
    let (_topic, timestamp_ms, key, value, headers) = record.into_parts();
    let headers = headers
        .into_iter()
        .map(|header| {
            let (name, value) = header.into_parts();
            MaterializationHeader::new(name, value)
        })
        .collect();
    MaterializationRecord::new(timestamp_ms, key, value, headers)
}

fn restore(
    records: &mut crate::producer::record_store::RecordStore,
    taken: Vec<(kafka_client_core::PayloadId, ProducerRecord)>,
) {
    for (payload_id, record) in taken {
        let restored = records.restore_after_materialization(payload_id, record);
        debug_assert!(restored.is_ok());
    }
}
