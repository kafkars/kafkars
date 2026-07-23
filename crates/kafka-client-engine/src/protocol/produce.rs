//! One-topic, one-partition Produce DTO and `RecordBatch` v2 materialization.

use bytes::Bytes;
use kafka_wire::{
    ProduceRequest,
    produce_request::{PartitionProduceData, TopicProduceData},
};
use kafka_wire_records::{
    Compression, Record, RecordBatch, RecordEncodeLimits, RecordHeader, TimestampType,
};

use super::error::ProduceMaterializationError;

const ACKS_ALL: i16 = -1;
const NO_LEADER_EPOCH: i32 = -1;
const NO_PRODUCER_ID: i64 = -1;
const NO_PRODUCER_EPOCH: i16 = -1;
const NO_SEQUENCE: i32 = -1;

/// Owned, policy-approved input for one explicit-partition Produce batch.
///
/// Core policy must validate the topic, partition, deadline, and capacity before
/// this execution input reaches materialization. Records retain admission order.
/// The broker timeout is an already-derived remaining budget, not a fresh
/// deadline owned here.
#[derive(Debug)]
pub(super) struct ExplicitProduceBatch {
    topic: String,
    partition: i32,
    records: Vec<ProduceRecord>,
    remaining_broker_timeout_ms: i32,
    max_batch_bytes: usize,
}

/// One engine-owned record in a policy-approved partition batch.
#[derive(Debug)]
pub(super) struct ProduceRecord {
    timestamp_ms: i64,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<ProduceHeader>,
}

/// One validated, non-null header name and its nullable engine-owned value.
#[derive(Debug)]
pub(super) struct ProduceHeader {
    name: String,
    value: Option<Bytes>,
}

impl ProduceHeader {
    /// Captures one validated UTF-8 header name and its nullable value.
    pub(super) const fn new(name: String, value: Option<Bytes>) -> Self {
        Self { name, value }
    }
}

impl ExplicitProduceBatch {
    /// Captures an ordered record run and its already-derived broker wait budget.
    pub(super) const fn new(
        topic: String,
        partition: i32,
        records: Vec<ProduceRecord>,
        remaining_broker_timeout_ms: i32,
        max_batch_bytes: usize,
    ) -> Self {
        Self {
            topic,
            partition,
            records,
            remaining_broker_timeout_ms,
            max_batch_bytes,
        }
    }
}

impl ProduceRecord {
    /// Captures one engine-owned record without changing nullable payloads.
    pub(super) const fn new(timestamp_ms: i64, key: Option<Bytes>, value: Option<Bytes>) -> Self {
        Self {
            timestamp_ms,
            key,
            value,
            headers: Vec::new(),
        }
    }

    /// Attaches headers in application order without deduplicating names.
    pub(super) fn with_headers(mut self, headers: Vec<ProduceHeader>) -> Self {
        self.headers = headers;
        self
    }
}

/// Opaque generated request plus its engine-owned encoded record bytes.
#[derive(Debug)]
pub(super) struct MaterializedProduce {
    request: ProduceRequest,
}

impl MaterializedProduce {
    /// Returns the retained `RecordBatch` bytes carried by this request.
    pub(super) fn retained_record_bytes(&self) -> usize {
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
pub(super) fn materialize_explicit_produce_batch(
    input: ExplicitProduceBatch,
) -> Result<MaterializedProduce, ProduceMaterializationError> {
    let ExplicitProduceBatch {
        topic: topic_name,
        partition: partition_index,
        records,
        remaining_broker_timeout_ms,
        max_batch_bytes,
    } = input;
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

fn record_batch(records: Vec<ProduceRecord>) -> Result<RecordBatch, ProduceMaterializationError> {
    let Some(base_timestamp) = records.first().map(|record| record.timestamp_ms) else {
        return Err(ProduceMaterializationError::empty_batch());
    };
    let last_offset = records.len().saturating_sub(1);
    let last_offset_delta = i32::try_from(last_offset)
        .map_err(|_| ProduceMaterializationError::record_count_overflow(records.len()))?;
    let max_timestamp = records
        .iter()
        .map(|record| record.timestamp_ms)
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
    record: ProduceRecord,
) -> Result<Record, ProduceMaterializationError> {
    let timestamp_delta = record
        .timestamp_ms
        .checked_sub(base_timestamp)
        .ok_or_else(|| {
            ProduceMaterializationError::timestamp_delta_overflow(
                base_timestamp,
                record.timestamp_ms,
            )
        })?;
    let offset_delta = i32::try_from(offset).map_err(|_| {
        ProduceMaterializationError::record_count_overflow(offset.saturating_add(1))
    })?;

    Ok(Record {
        attributes: 0,
        timestamp_delta,
        offset_delta,
        key: record.key,
        value: record.value,
        headers: record
            .headers
            .into_iter()
            .map(|header| RecordHeader {
                key: header.name.into(),
                value: header.value,
            })
            .collect(),
    })
}
