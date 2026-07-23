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

/// Owned, policy-approved input for the first explicit-partition Produce slice.
///
/// Core policy must validate the topic, partition, deadline, and capacity before
/// this execution input reaches materialization. The broker timeout is the
/// already-derived remaining budget, not a fresh deadline owned here.
#[derive(Debug)]
pub(super) struct ExplicitProduce {
    topic: String,
    partition: i32,
    timestamp_ms: i64,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<ProduceHeader>,
    remaining_broker_timeout_ms: i32,
    max_batch_bytes: usize,
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

impl ExplicitProduce {
    /// Captures one admitted record and its already-derived broker wait budget.
    pub(super) const fn new(
        topic: String,
        partition: i32,
        timestamp_ms: i64,
        key: Option<Bytes>,
        value: Option<Bytes>,
        remaining_broker_timeout_ms: i32,
        max_batch_bytes: usize,
    ) -> Self {
        Self {
            topic,
            partition,
            timestamp_ms,
            key,
            value,
            headers: Vec::new(),
            remaining_broker_timeout_ms,
            max_batch_bytes,
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

/// Uses the sibling wire crates to materialize one uncompressed Produce request.
pub(super) fn materialize_explicit_produce(
    input: ExplicitProduce,
) -> Result<MaterializedProduce, ProduceMaterializationError> {
    let ExplicitProduce {
        topic: topic_name,
        partition: partition_index,
        timestamp_ms,
        key,
        value,
        headers,
        remaining_broker_timeout_ms,
        max_batch_bytes,
    } = input;
    let records = record_batch(timestamp_ms, key, value, headers)
        .encode_to_bytes(RecordEncodeLimits::new(max_batch_bytes, max_batch_bytes))
        .map_err(ProduceMaterializationError::new)?;

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
    timestamp_ms: i64,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<ProduceHeader>,
) -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        last_offset_delta: 0,
        partition_leader_epoch: NO_LEADER_EPOCH,
        compression: Compression::None,
        timestamp_type: TimestampType::CreateTime,
        is_transactional: false,
        is_control: false,
        has_delete_horizon: false,
        base_timestamp: timestamp_ms,
        max_timestamp: timestamp_ms,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        base_sequence: NO_SEQUENCE,
        records: vec![Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
            key,
            value,
            headers: headers
                .into_iter()
                .map(|header| RecordHeader {
                    key: header.name.into(),
                    value: header.value,
                })
                .collect(),
        }],
    }
}
