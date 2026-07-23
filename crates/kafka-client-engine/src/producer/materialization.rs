//! Producer-owned, wire-free transfer values for one sealed batch.

use bytes::Bytes;

/// One header transferred from engine retention to protocol materialization.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MaterializationHeader {
    name: String,
    value: Option<Bytes>,
}

impl MaterializationHeader {
    pub(crate) const fn new(name: String, value: Option<Bytes>) -> Self {
        Self { name, value }
    }

    /// Consumes the header into the protocol adapter's mechanical fields.
    pub(crate) fn into_parts(self) -> (String, Option<Bytes>) {
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
    topic: String,
    partition: i32,
    records: Vec<MaterializationRecord>,
    remaining_broker_timeout_ms: i32,
    max_batch_bytes: usize,
}

impl MaterializationBatch {
    pub(crate) const fn new(
        topic: String,
        partition: i32,
        records: Vec<MaterializationRecord>,
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

    /// Consumes the batch into the existing protocol materializer's fields.
    pub(crate) fn into_parts(self) -> (String, i32, Vec<MaterializationRecord>, i32, usize) {
        (
            self.topic,
            self.partition,
            self.records,
            self.remaining_broker_timeout_ms,
            self.max_batch_bytes,
        )
    }
}
