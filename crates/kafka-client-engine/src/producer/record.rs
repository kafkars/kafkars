//! Bytes-native producer records retained exclusively by the engine.

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::PartitionIndex;

use super::ProducerStoreError;

/// One ordered Kafka header with a non-null name and nullable value.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ProducerHeader {
    name: String,
    value: Option<Bytes>,
}

impl ProducerHeader {
    /// Captures a validated header name and its nullable bytes.
    pub(crate) const fn new(name: String, value: Option<Bytes>) -> Self {
        Self { name, value }
    }

    pub(super) fn retained_bytes(&self) -> Result<usize, ProducerStoreError> {
        self.name
            .len()
            .checked_add(self.value.as_ref().map_or(0, Bytes::len))
            .ok_or(ProducerStoreError::RetainedSizeOverflow)
    }

    pub(super) fn into_parts(self) -> (String, Option<Bytes>) {
        (self.name, self.value)
    }
}

/// Complete engine-owned application record before Kafka batch encoding.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ProducerRecord {
    topic: Arc<str>,
    partition: PartitionIndex,
    timestamp_ms: i64,
    defaulted_timestamp: bool,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<ProducerHeader>,
}

pub(super) struct ProducerRecordParts {
    pub(super) topic: Arc<str>,
    pub(super) partition: PartitionIndex,
    pub(super) timestamp_ms: i64,
    pub(super) defaulted_timestamp: bool,
    pub(super) key: Option<Bytes>,
    pub(super) value: Option<Bytes>,
    pub(super) headers: Vec<ProducerHeader>,
}

impl ProducerRecord {
    /// Captures one validated explicit-partition record without encoding it.
    pub(crate) const fn new(
        topic: Arc<str>,
        partition: PartitionIndex,
        timestamp_ms: i64,
        key: Option<Bytes>,
        value: Option<Bytes>,
    ) -> Self {
        Self {
            topic,
            partition,
            timestamp_ms,
            defaulted_timestamp: false,
            key,
            value,
            headers: Vec::new(),
        }
    }

    pub(super) const fn from_public(
        topic: Arc<str>,
        partition: PartitionIndex,
        timestamp_ms: i64,
        defaulted_timestamp: bool,
        key: Option<Bytes>,
        value: Option<Bytes>,
        headers: Vec<ProducerHeader>,
    ) -> Self {
        Self {
            topic,
            partition,
            timestamp_ms,
            defaulted_timestamp,
            key,
            value,
            headers,
        }
    }

    /// Attaches headers in application order without deduplicating names.
    pub(crate) fn with_headers(mut self, headers: Vec<ProducerHeader>) -> Self {
        self.headers = headers;
        self
    }

    pub(super) fn retained_bytes(&self) -> Result<usize, ProducerStoreError> {
        let fields = self
            .topic
            .len()
            .checked_add(self.key.as_ref().map_or(0, Bytes::len))
            .and_then(|size| size.checked_add(self.value.as_ref().map_or(0, Bytes::len)))
            .ok_or(ProducerStoreError::RetainedSizeOverflow)?;
        self.headers.iter().try_fold(fields, |size, header| {
            size.checked_add(header.retained_bytes()?)
                .ok_or(ProducerStoreError::RetainedSizeOverflow)
        })
    }

    pub(super) fn topic(&self) -> &Arc<str> {
        &self.topic
    }

    pub(super) const fn partition(&self) -> PartitionIndex {
        self.partition
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        Arc<str>,
        i64,
        Option<Bytes>,
        Option<Bytes>,
        Vec<ProducerHeader>,
    ) {
        (
            self.topic,
            self.timestamp_ms,
            self.key,
            self.value,
            self.headers,
        )
    }

    pub(super) fn into_public_parts(self) -> ProducerRecordParts {
        ProducerRecordParts {
            topic: self.topic,
            partition: self.partition,
            timestamp_ms: self.timestamp_ms,
            defaulted_timestamp: self.defaulted_timestamp,
            key: self.key,
            value: self.value,
            headers: self.headers,
        }
    }
}
