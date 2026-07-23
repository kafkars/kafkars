//! One-topic, one-partition Produce DTO and `RecordBatch` v2 materialization.

use bytes::Bytes;
use kafka_wire::{
    ProduceRequest,
    produce_request::{PartitionProduceData, TopicProduceData},
};
use kafka_wire_records::{
    Compression, Record, RecordBatch, RecordEncodeLimits, RecordHeader, TimestampType,
};

use crate::producer::{MaterializationBatch, MaterializationRecord};

use super::error::ProduceMaterializationError;

const ACKS_ALL: i16 = -1;
const NO_LEADER_EPOCH: i32 = -1;
const NO_PRODUCER_ID: i64 = -1;
const NO_PRODUCER_EPOCH: i16 = -1;
const NO_SEQUENCE: i32 = -1;

/// Opaque generated request plus separately bounded host-owned encoded bytes.
///
/// `ProducerStore` continues accounting for accepted application payloads until
/// core emits their release effects. The host must reserve and retain this
/// encoded request independently until driver settlement; this type deliberately
/// does not introduce an unbounded materialized-batch registry.
#[derive(Debug)]
pub(crate) struct MaterializedProduce {
    request: ProduceRequest,
}

impl MaterializedProduce {
    /// Returns the retained `RecordBatch` bytes carried by this request.
    pub(crate) fn retained_record_bytes(&self) -> usize {
        self.request
            .topic_data
            .first()
            .and_then(|topic| topic.partition_data.first())
            .and_then(|partition| partition.records.as_ref())
            .map_or(0, Bytes::len)
    }

    #[cfg(test)]
    pub(super) const fn request(&self) -> &ProduceRequest {
        &self.request
    }
}

/// Uses the sibling wire crates to materialize one uncompressed Produce batch.
pub(crate) fn materialize_explicit_produce_batch(
    input: MaterializationBatch,
) -> Result<MaterializedProduce, ProduceMaterializationError> {
    let (topic_name, partition_index, records, remaining_broker_timeout_ms, max_batch_bytes) =
        input.into_parts();
    let records = record_batch(records)?
        .encode_to_bytes(RecordEncodeLimits::new(max_batch_bytes, max_batch_bytes))
        .map_err(ProduceMaterializationError::record)?;

    let mut partition = PartitionProduceData::default();
    partition.index = partition_index;
    partition.records = Some(records);

    let mut topic = TopicProduceData::default();
    topic.name = topic_name.into();
    topic.partition_data.push(partition);

    let mut request = ProduceRequest::default();
    request.acks = ACKS_ALL;
    request.timeout_ms = remaining_broker_timeout_ms;
    request.topic_data.push(topic);
    Ok(MaterializedProduce { request })
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

    Ok(Record {
        attributes: 0,
        timestamp_delta,
        offset_delta,
        key,
        value,
        headers: headers
            .into_iter()
            .map(|header| {
                let (name, value) = header.into_parts();
                RecordHeader {
                    key: name.into(),
                    value,
                }
            })
            .collect(),
    })
}
