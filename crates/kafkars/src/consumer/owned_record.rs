//! Lease-preserving owned record path for directly assigned consumers.

use std::sync::Arc;

use crate::{Record, bridge::consumer as bridge};

/// Consuming iterator over non-clone records sharing one delivery lease.
#[must_use = "dropping the iterator releases its share of the delivery lease"]
#[derive(Debug)]
pub struct OwnedConsumerRecords {
    inner: bridge::AssignedConsumerOwnedRecords,
}

impl OwnedConsumerRecords {
    pub(super) const fn from_bridge(inner: bridge::AssignedConsumerOwnedRecords) -> Self {
        Self { inner }
    }
}

impl Iterator for OwnedConsumerRecords {
    type Item = OwnedConsumerRecord;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(OwnedConsumerRecord::from_bridge)
    }
}

/// One non-clone direct-consumer record retaining its exact delivery lease.
#[must_use = "drop or transfer the record together with its delivery lease"]
#[derive(Debug)]
pub struct OwnedConsumerRecord {
    inner: bridge::AssignedConsumerOwnedRecord,
}

impl OwnedConsumerRecord {
    const fn from_bridge(inner: bridge::AssignedConsumerOwnedRecord) -> Self {
        Self { inner }
    }

    /// Returns the retained source Kafka topic name.
    pub fn topic(&self) -> &str {
        self.inner.topic()
    }

    /// Returns the source partition.
    pub fn partition(&self) -> i32 {
        self.inner.partition()
    }

    /// Returns the absolute source log offset.
    pub fn offset(&self) -> i64 {
        self.inner.offset()
    }

    /// Returns the source timestamp in milliseconds when present.
    pub fn timestamp_millis(&self) -> Option<i64> {
        self.inner.timestamp_millis()
    }

    /// Returns the nullable key without exposing a clonable byte owner.
    pub fn key(&self) -> Option<&[u8]> {
        self.inner.key()
    }

    /// Returns the nullable value without exposing a clonable byte owner.
    pub fn value(&self) -> Option<&[u8]> {
        self.inner.value()
    }

    /// Returns duplicate-preserving borrowed headers in broker order.
    pub fn headers(&self) -> impl ExactSizeIterator<Item = OwnedConsumerHeader<'_>> {
        self.inner.headers().map(OwnedConsumerHeader::from_bridge)
    }

    /// Converts this source record into a producer record without copying bytes.
    ///
    /// The destination starts without an explicit partition so the caller may
    /// select one afterward. Timestamp, key, value, header order, duplicates,
    /// nulls, and empty values are preserved. The source delivery lease stays
    /// attached through producer or transactional admission and rejection.
    pub fn into_record(self, target_topic: impl Into<Arc<str>>) -> Record {
        self.inner.into_record(target_topic.into())
    }
}

/// One header borrowing its bytes from a lease-preserving owned record.
#[derive(Debug)]
pub struct OwnedConsumerHeader<'record> {
    inner: bridge::AssignedConsumerOwnedHeader<'record>,
}

impl<'record> OwnedConsumerHeader<'record> {
    const fn from_bridge(inner: bridge::AssignedConsumerOwnedHeader<'record>) -> Self {
        Self { inner }
    }

    /// Returns validated UTF-8 header-name bytes.
    pub fn key(&self) -> &[u8] {
        self.inner.key()
    }

    /// Returns the nullable value without conflating null and empty bytes.
    pub fn value(&self) -> Option<&[u8]> {
        self.inner.value()
    }
}
