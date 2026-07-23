//! Timeout-free `RecordBatch` bytes and late-bound generated Produce requests.

use std::sync::Arc;

use bytes::Bytes;
use kafka_wire::{
    ProduceRequest,
    produce_request::{PartitionProduceData, TopicProduceData},
};
use kafka_wire_records::{
    Compression, Record, RecordBatch, RecordEncodeLimits, RecordHeader, TimestampType,
};

use crate::producer::materialization::{MaterializationBatch, MaterializationRecord};

use super::error::ProduceMaterializationError;

const ACKS_ALL: i16 = -1;
const NO_LEADER_EPOCH: i32 = -1;
const NO_PRODUCER_ID: i64 = -1;
const NO_PRODUCER_EPOCH: i16 = -1;
const NO_SEQUENCE: i32 = -1;

/// Opaque route and separately bounded host-owned encoded batch bytes.
///
/// `ProducerStore` continues accounting for accepted application payloads until
/// core emits their release effects. The host must reserve and retain this
/// encoded batch independently until driver settlement; this type deliberately
/// contains no deadline-derived request timeout.
#[derive(Debug)]
pub(crate) struct MaterializedProduce {
    topic: Arc<str>,
    partition: i32,
    records: Bytes,
}

impl MaterializedProduce {
    /// Returns the retained `RecordBatch` bytes awaiting driver submission.
    pub(crate) fn retained_record_bytes(&self) -> usize {
        self.records.len()
    }

    /// Consumes encoded bytes into one name-routed request at submission time.
    pub(crate) fn into_name_routed_request(
        self,
        remaining_broker_timeout_ms: i32,
    ) -> ProduceRequest {
        let mut partition = PartitionProduceData::default();
        partition.index = self.partition;
        partition.records = Some(self.records);

        let mut topic = TopicProduceData::default();
        topic.name = self.topic.as_ref().into();
        topic.partition_data.push(partition);

        let mut request = ProduceRequest::default();
        request.acks = ACKS_ALL;
        request.timeout_ms = remaining_broker_timeout_ms;
        request.topic_data.push(topic);
        request
    }

    #[cfg(test)]
    pub(super) const fn encoded_records(&self) -> &Bytes {
        &self.records
    }
}

/// Uses the sibling wire crates to materialize one uncompressed Produce batch.
pub(crate) fn materialize_explicit_produce_batch(
    input: MaterializationBatch,
) -> Result<MaterializedProduce, ProduceMaterializationError> {
    let (topic, partition, records, max_batch_bytes) = input.into_parts();
    let records = record_batch(records)?
        .encode_to_bytes(RecordEncodeLimits::new(max_batch_bytes, max_batch_bytes))
        .map_err(ProduceMaterializationError::record)?;

    Ok(MaterializedProduce {
        topic,
        partition,
        records,
    })
}

fn record_batch(
    records: Vec<MaterializationRecord>,
) -> Result<RecordBatch, ProduceMaterializationError> {
    let Some(base_timestamp) = records
        .first()
        .map(MaterializationRecord::timestamp_ms_for_protocol)
    else {
        return Err(ProduceMaterializationError::empty_batch());
    };
    let last_offset = records.len().saturating_sub(1);
    let last_offset_delta = i32::try_from(last_offset)
        .map_err(|_| ProduceMaterializationError::record_count_overflow(records.len()))?;
    let max_timestamp = records
        .iter()
        .map(MaterializationRecord::timestamp_ms_for_protocol)
        .max()
        .unwrap_or(base_timestamp);
    let records = records
        .into_iter()
        .enumerate()
        .map(|(offset, record)| wire_record(base_timestamp, offset, record))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RecordBatch {
        base_offset: 0,
        last_offset_delta,
        partition_leader_epoch: NO_LEADER_EPOCH,
        compression: Compression::None,
        timestamp_type: TimestampType::CreateTime,
        is_transactional: false,
        is_control: false,
        has_delete_horizon: false,
        base_timestamp,
        max_timestamp,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        base_sequence: NO_SEQUENCE,
        records,
    })
}

fn wire_record(
    base_timestamp: i64,
    offset: usize,
    record: MaterializationRecord,
) -> Result<Record, ProduceMaterializationError> {
    let (timestamp_ms, key, value, headers) = record.into_parts();
    let timestamp_delta = timestamp_ms.checked_sub(base_timestamp).ok_or_else(|| {
        ProduceMaterializationError::timestamp_delta_overflow(base_timestamp, timestamp_ms)
    })?;
    let offset_delta = i32::try_from(offset).map_err(|_| {
        ProduceMaterializationError::record_count_overflow(offset.saturating_add(1))
    })?;

    let headers = headers
        .into_iter()
        .map(|header| {
            let (name, value) = header.into_parts();
            Ok(RecordHeader {
                key: name
                    .try_into()
                    .map_err(ProduceMaterializationError::invalid_header_name)?,
                value,
            })
        })
        .collect::<Result<Vec<_>, ProduceMaterializationError>>()?;

    Ok(Record {
        attributes: 0,
        timestamp_delta,
        offset_delta,
        key,
        value,
        headers,
    })
}
