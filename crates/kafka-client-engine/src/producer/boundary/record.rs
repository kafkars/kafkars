//! Stable bytes-native producer records at the public engine boundary.

mod header;
#[cfg(test)]
mod header_test;

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::PartitionIndex;

use super::super::record::{
    ProducerRecord as StoredProducerRecord, ProducerRecordParts, ProducerSourceOwner,
};

pub use header::ProducerHeader;

/// One engine-owned bytes-native record before producer admission.
#[derive(Debug, Eq, PartialEq)]
pub struct ProducerRecord {
    pub(super) topic: Arc<str>,
    pub(super) partition: Option<i32>,
    pub(super) timestamp_ms: Option<i64>,
    pub(super) key: Option<Bytes>,
    pub(super) value: Option<Bytes>,
    pub(super) headers: Vec<ProducerHeader>,
    pub(super) source_owner: ProducerSourceOwner,
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
            source_owner: ProducerSourceOwner::none(),
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

    /// Attaches an opaque upstream byte lease through admission or rejection.
    #[doc(hidden)]
    pub fn retain_source_owner(mut self, owner: Arc<dyn Send + Sync>) -> Self {
        self.source_owner = ProducerSourceOwner::new(owner);
        self
    }

    /// Transfers the exact public record fields and any opaque source lease.
    #[doc(hidden)]
    #[allow(
        clippy::type_complexity,
        reason = "private facade transfer is one exact record"
    )]
    pub fn into_shared_parts(
        self,
    ) -> (
        Arc<str>,
        Option<i32>,
        Option<i64>,
        Option<Bytes>,
        Option<Bytes>,
        Vec<ProducerHeader>,
        Option<Arc<dyn Send + Sync>>,
    ) {
        (
            self.topic,
            self.partition,
            self.timestamp_ms,
            self.key,
            self.value,
            self.headers,
            self.source_owner.into_inner(),
        )
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
            .map(ProducerHeader::into_stored)
            .collect();
        StoredProducerRecord::from_public(ProducerRecordParts {
            topic: self.topic,
            partition,
            timestamp_ms,
            defaulted_timestamp,
            key: self.key,
            value: self.value,
            headers,
            source_owner: self.source_owner,
        })
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
            source_owner,
        } = record.into_public_parts();
        let headers = headers
            .into_iter()
            .map(ProducerHeader::from_stored)
            .collect();
        Self {
            topic,
            partition: partition.and_then(|partition| i32::try_from(partition.get()).ok()),
            timestamp_ms: (!defaulted_timestamp).then_some(timestamp_ms),
            key,
            value,
            headers,
            source_owner,
        }
    }
}
