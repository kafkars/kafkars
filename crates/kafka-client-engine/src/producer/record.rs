//! Bytes-native producer records retained exclusively by the engine.

mod header;
#[cfg(test)]
mod header_test;
mod partitioning;

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::PartitionIndex;

use super::ProducerStoreError;
use super::materialization::MaterializationRecord;

#[cfg(test)]
pub(super) use header::HEADER_BYTES_OWNER_CONTROL_BYTES;
pub(super) use header::HEADER_CONTROL_BYTES;
pub(in crate::producer) use header::{ProducerHeader, ProducerSourceOwner};

/// Complete engine-owned application record before Kafka batch encoding.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ProducerRecord {
    topic: Arc<str>,
    partition: Option<PartitionIndex>,
    leader_broker_id: Option<i32>,
    automatic_partition: bool,
    timestamp_ms: i64,
    defaulted_timestamp: bool,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<ProducerHeader>,
    source_owner: ProducerSourceOwner,
}

pub(super) struct ProducerRecordParts {
    pub(super) topic: Arc<str>,
    pub(super) partition: Option<PartitionIndex>,
    pub(super) timestamp_ms: i64,
    pub(super) defaulted_timestamp: bool,
    pub(super) key: Option<Bytes>,
    pub(super) value: Option<Bytes>,
    pub(super) headers: Vec<ProducerHeader>,
    pub(super) source_owner: ProducerSourceOwner,
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
            partition: Some(partition),
            leader_broker_id: None,
            automatic_partition: false,
            timestamp_ms,
            defaulted_timestamp: false,
            key,
            value,
            headers: Vec::new(),
            source_owner: ProducerSourceOwner::none(),
        }
    }

    pub(super) fn from_public(parts: ProducerRecordParts) -> Self {
        let automatic_partition = parts.partition.is_none();
        Self {
            topic: parts.topic,
            partition: parts.partition,
            leader_broker_id: None,
            automatic_partition,
            timestamp_ms: parts.timestamp_ms,
            defaulted_timestamp: parts.defaulted_timestamp,
            key: parts.key,
            value: parts.value,
            headers: parts.headers,
            source_owner: parts.source_owner,
        }
    }

    /// Attaches headers in application order without deduplicating names.
    pub(in crate::producer) fn with_headers(mut self, headers: Vec<ProducerHeader>) -> Self {
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
        let fields = self.headers.iter().try_fold(fields, |size, header| {
            size.checked_add(header.retained_bytes()?)
                .ok_or(ProducerStoreError::RetainedSizeOverflow)
        })?;
        let controls = header_control_bytes(self.headers.len(), self.headers.capacity())?;
        fields
            .checked_add(controls)
            .ok_or(ProducerStoreError::RetainedSizeOverflow)
    }

    pub(super) fn topic(&self) -> &Arc<str> {
        &self.topic
    }

    pub(super) fn materialization_view(&self) -> MaterializationRecord {
        let headers = self
            .headers
            .iter()
            .map(ProducerHeader::materialization_view)
            .collect();
        MaterializationRecord::new(
            self.timestamp_ms,
            self.key.clone(),
            self.value.clone(),
            headers,
        )
    }

    pub(super) fn release_source_owner(&mut self) {
        self.source_owner.release();
        for header in &mut self.headers {
            header.release_source_owner();
        }
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
}

pub(super) fn header_control_bytes(
    count: usize,
    capacity: usize,
) -> Result<usize, ProducerStoreError> {
    i32::try_from(count).map_err(|_| ProducerStoreError::HeaderCountOutOfRange)?;
    capacity
        .checked_mul(HEADER_CONTROL_BYTES)
        .ok_or(ProducerStoreError::RetainedSizeOverflow)
}
