//! Stable bytes-native producer records at the public engine boundary.

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::PartitionIndex;

use super::super::record::{
    ProducerHeader as StoredProducerHeader, ProducerRecord as StoredProducerRecord,
    ProducerRecordParts,
};

/// One ordered Kafka header with a non-null name and nullable bytes.
#[derive(Debug, Eq, PartialEq)]
pub struct ProducerHeader {
    name: String,
    value: Option<Bytes>,
}

impl ProducerHeader {
    /// Creates a header with a non-null value.
    pub fn new(name: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self {
            name: name.into(),
            value: Some(value.into()),
        }
    }

    /// Creates a header with a null value.
    pub fn null(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: None,
        }
    }

    /// Returns the header name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the nullable header bytes.
    pub fn value(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }
}

/// One engine-owned bytes-native record before producer admission.
#[derive(Debug, Eq, PartialEq)]
pub struct ProducerRecord {
    topic: Arc<str>,
    partition: Option<i32>,
    timestamp_ms: Option<i64>,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<ProducerHeader>,
}

impl ProducerRecord {
    /// Begins a record for the named topic.
    pub fn to(topic: impl Into<Arc<str>>) -> Self {
        Self {
            topic: topic.into(),
            partition: None,
            timestamp_ms: None,
            key: None,
            value: None,
            headers: Vec::new(),
        }
    }

    /// Selects an explicit zero-based partition.
    pub const fn partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Selects an explicit Kafka record timestamp in Unix epoch milliseconds.
    pub const fn timestamp_milliseconds(mut self, timestamp_ms: i64) -> Self {
        self.timestamp_ms = Some(timestamp_ms);
        self
    }

    /// Sets a non-null key.
    pub fn key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Sets a non-null value. Absence remains an explicit tombstone.
    pub fn value(mut self, value: impl Into<Bytes>) -> Self {
        self.value = Some(value.into());
        self
    }

    /// Appends an ordered, duplicate-preserving header.
    pub fn header(mut self, header: ProducerHeader) -> Self {
        self.headers.push(header);
        self
    }

    /// Returns the logical topic.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the explicit partition, when supplied.
    pub const fn explicit_partition(&self) -> Option<i32> {
        self.partition
    }

    /// Returns the caller-supplied timestamp, when supplied.
    pub const fn timestamp(&self) -> Option<i64> {
        self.timestamp_ms
    }

    /// Returns nullable key bytes.
    pub fn key_bytes(&self) -> Option<&Bytes> {
        self.key.as_ref()
    }

    /// Returns nullable value bytes.
    pub fn value_bytes(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }

    /// Returns ordered, duplicate-preserving headers.
    pub fn headers(&self) -> &[ProducerHeader] {
        &self.headers
    }

    pub(super) fn validate_explicit_partition(&self) -> Option<PartitionIndex> {
        let raw = u32::try_from(self.partition?).ok()?;
        Some(PartitionIndex::from_raw(raw))
    }

    pub(super) fn into_stored(
        self,
        partition: Option<PartitionIndex>,
        default_timestamp_ms: i64,
    ) -> StoredProducerRecord {
        let defaulted_timestamp = self.timestamp_ms.is_none();
        let timestamp_ms = self.timestamp_ms.unwrap_or(default_timestamp_ms);
        let headers = self
            .headers
            .into_iter()
            .map(|header| StoredProducerHeader::new(header.name, header.value))
            .collect();
        StoredProducerRecord::from_public(
            self.topic,
            partition,
            timestamp_ms,
            defaulted_timestamp,
            self.key,
            self.value,
            headers,
        )
    }

    pub(super) fn from_stored(record: StoredProducerRecord) -> Self {
        let ProducerRecordParts {
            topic,
            partition,
            timestamp_ms,
            defaulted_timestamp,
            key,
            value,
            headers,
        } = record.into_public_parts();
        let headers = headers
            .into_iter()
            .map(|header| {
                let (name, value) = header.into_parts();
                ProducerHeader { name, value }
            })
            .collect();
        Self {
            topic,
            partition: partition.and_then(|partition| i32::try_from(partition.get()).ok()),
            timestamp_ms: (!defaulted_timestamp).then_some(timestamp_ms),
            key,
            value,
            headers,
        }
    }
}
