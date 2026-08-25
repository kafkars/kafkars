//! Producer-owned, wire-free shared-handle views of one sealed batch.
//!
//! Canonical raw payload backing remains charged in the record store. Each
//! disposable view adds only bounded `Vec` and shared-handle control metadata;
//! encoded `RecordBatch` bytes are charged separately by prepared execution.

mod batch;
mod transactional_batch;

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::{
    ProducerIdentity, ProducerSequenceLease, TransactionSequenceLease,
    TransactionalProducerIdentity, partitioning::TopicMetadataGeneration,
};

/// One header transferred from engine retention to protocol materialization.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MaterializationHeader {
    name: Bytes,
    value: Option<Bytes>,
}

impl MaterializationHeader {
    pub(crate) fn new(name: impl Into<Bytes>, value: Option<Bytes>) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }

    /// Consumes the header into the protocol adapter's mechanical fields.
    pub(crate) fn into_parts(self) -> (Bytes, Option<Bytes>) {
        (self.name, self.value)
    }
}

/// One ordered record transferred without changing nullable byte fields.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MaterializationRecord {
    timestamp_ms: i64,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<MaterializationHeader>,
}

impl MaterializationRecord {
    pub(crate) const fn new(
        timestamp_ms: i64,
        key: Option<Bytes>,
        value: Option<Bytes>,
        headers: Vec<MaterializationHeader>,
    ) -> Self {
        Self {
            timestamp_ms,
            key,
            value,
            headers,
        }
    }

    /// Returns the timestamp needed to derive Kafka batch deltas.
    pub(crate) const fn timestamp_ms_for_protocol(&self) -> i64 {
        self.timestamp_ms
    }

    /// Borrows the serialized key bytes used by deterministic partition policy.
    pub(crate) fn key_bytes(&self) -> Option<&Bytes> {
        self.key.as_ref()
    }

    /// Consumes the record into the protocol adapter's mechanical fields.
    pub(crate) fn into_parts(
        self,
    ) -> (
        i64,
        Option<Bytes>,
        Option<Bytes>,
        Vec<MaterializationHeader>,
    ) {
        (self.timestamp_ms, self.key, self.value, self.headers)
    }
}

/// One explicit-partition batch ready for `kafka-wire-records`.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MaterializationBatch {
    topic: Arc<str>,
    expected_topic_uuid: Option<[u8; 16]>,
    validated_topic_generation: Option<TopicMetadataGeneration>,
    partition: i32,
    leader_broker_id: Option<i32>,
    records: Vec<MaterializationRecord>,
    max_batch_bytes: usize,
    source_retained_bytes: usize,
    identity: ProducerIdentity,
    sequence: ProducerSequenceLease,
}

/// One transactional batch whose identity and sequence were fenced before encoding.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct TransactionalMaterializationBatch {
    topic: Arc<str>,
    expected_topic_uuid: Option<[u8; 16]>,
    validated_topic_generation: Option<TopicMetadataGeneration>,
    partition: i32,
    records: Vec<MaterializationRecord>,
    max_batch_bytes: usize,
    identity: TransactionalProducerIdentity,
    sequence: TransactionSequenceLease,
}
