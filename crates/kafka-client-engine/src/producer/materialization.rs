//! Producer-owned, wire-free shared-handle views of one sealed batch.
//!
//! Canonical raw payload backing remains charged in the record store. Each
//! disposable view adds only bounded `Vec` and shared-handle control metadata;
//! encoded `RecordBatch` bytes are charged separately by prepared execution.

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::{
    ProducerIdentity, ProducerSequenceLease, TransactionSequenceLease,
    TransactionalProducerIdentity,
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
    partition: i32,
    records: Vec<MaterializationRecord>,
    max_batch_bytes: usize,
    identity: TransactionalProducerIdentity,
    sequence: TransactionSequenceLease,
}

impl TransactionalMaterializationBatch {
    pub(crate) fn new(
        topic: impl Into<Arc<str>>,
        partition: i32,
        records: Vec<MaterializationRecord>,
        max_batch_bytes: usize,
        identity: TransactionalProducerIdentity,
        sequence: TransactionSequenceLease,
    ) -> Self {
        Self {
            topic: topic.into(),
            partition,
            records,
            max_batch_bytes,
            identity,
            sequence,
        }
    }

    /// Borrows the exact name used by partition enrollment and Produce routing.
    pub(crate) fn topic(&self) -> &Arc<str> {
        &self.topic
    }

    /// Returns the exact partition used by enrollment, sequencing, and Produce.
    pub(crate) const fn partition(&self) -> i32 {
        self.partition
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Arc<str>,
        i32,
        Vec<MaterializationRecord>,
        usize,
        TransactionalProducerIdentity,
        TransactionSequenceLease,
    ) {
        (
            self.topic,
            self.partition,
            self.records,
            self.max_batch_bytes,
            self.identity,
            self.sequence,
        )
    }

    pub(crate) const fn identity(&self) -> TransactionalProducerIdentity {
        self.identity
    }
}

impl MaterializationBatch {
    #[cfg(test)]
    pub(crate) fn try_for_test(
        topic: impl Into<Arc<str>>,
        partition: i32,
        records: Vec<MaterializationRecord>,
        max_batch_bytes: usize,
    ) -> Option<Self> {
        let identity = ProducerIdentity::try_new(1, 0)?;
        let count = u32::try_from(records.len().max(1)).ok()?;
        let sequence = ProducerSequenceLease::try_new(0, count)?;
        Some(Self::idempotent(
            topic,
            partition,
            None,
            records,
            max_batch_bytes,
            max_batch_bytes,
            identity,
            sequence,
        ))
    }

    pub(crate) fn idempotent(
        topic: impl Into<Arc<str>>,
        partition: i32,
        leader_broker_id: Option<i32>,
        records: Vec<MaterializationRecord>,
        max_batch_bytes: usize,
        source_retained_bytes: usize,
        identity: ProducerIdentity,
        sequence: ProducerSequenceLease,
    ) -> Self {
        Self {
            topic: topic.into(),
            partition,
            leader_broker_id,
            records,
            max_batch_bytes,
            source_retained_bytes,
            identity,
            sequence,
        }
    }

    /// Consumes the batch into the existing protocol materializer's fields.
    pub(crate) fn into_idempotent_parts(
        self,
    ) -> (
        Arc<str>,
        i32,
        Option<i32>,
        Vec<MaterializationRecord>,
        usize,
        ProducerIdentity,
        ProducerSequenceLease,
    ) {
        (
            self.topic,
            self.partition,
            self.leader_broker_id,
            self.records,
            self.max_batch_bytes,
            self.identity,
            self.sequence,
        )
    }

    /// Returns the canonical source bytes fenced while a worker owns shared views.
    pub(crate) const fn source_retained_bytes(&self) -> usize {
        self.source_retained_bytes
    }

    /// Returns the maximum encoded output retained by this exact job.
    pub(crate) const fn max_batch_bytes(&self) -> usize {
        self.max_batch_bytes
    }

    #[cfg(test)]
    pub(crate) fn into_parts(self) -> (Arc<str>, i32, Vec<MaterializationRecord>, usize) {
        (
            self.topic,
            self.partition,
            self.records,
            self.max_batch_bytes,
        )
    }
}
