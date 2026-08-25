//! Lease-preserving owned record path for directly assigned consumers.

use std::{fmt, sync::Arc};

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

    /// Fallibly transfers this record into producer input and retained source evidence.
    ///
    /// The caller supplies an already-owned target topic so this boundary does
    /// not hide a topic allocation. Header capacity is reserved before any
    /// shared byte handle is cloned. Allocation failure returns the exact source
    /// record and target owner in [`TransferRejection`].
    ///
    /// The destination starts without an explicit partition. Timestamp, key,
    /// value, header order, duplicates, nulls, and empty values are preserved
    /// without copying their bytes. The returned [`RetainedSourceRecord`] owns
    /// the original Fetch lease; keep it through accepted send observation and
    /// any larger transaction or scheduling terminal that must retain source
    /// evidence and its byte charge.
    pub fn try_into_record(
        self,
        target_topic: Arc<str>,
    ) -> Result<(Record, RetainedSourceRecord), TransferRejection> {
        match self.inner.try_into_record(target_topic) {
            Ok((record, source)) => Ok((
                record,
                RetainedSourceRecord {
                    inner: Self::from_bridge(source),
                },
            )),
            Err((source, target_topic)) => Err(TransferRejection {
                source: Self::from_bridge(source),
                target_topic,
            }),
        }
    }
}

/// Linear source record retained independently from its outgoing producer record.
#[must_use = "retain source evidence through the operation terminal or drop it explicitly"]
#[derive(Debug)]
pub struct RetainedSourceRecord {
    inner: OwnedConsumerRecord,
}

impl RetainedSourceRecord {
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
        self.inner.headers()
    }
}

/// Allocation rejection retaining every owner supplied to record transfer.
#[must_use = "recover the exact source record and target topic owner"]
pub struct TransferRejection {
    source: OwnedConsumerRecord,
    target_topic: Arc<str>,
}

impl TransferRejection {
    /// Borrows the exact source record that did not transfer.
    pub const fn record(&self) -> &OwnedConsumerRecord {
        &self.source
    }

    /// Returns the target topic name whose owner did not transfer.
    pub fn target_topic(&self) -> &str {
        &self.target_topic
    }

    /// Recovers the exact source record.
    pub fn into_record(self) -> OwnedConsumerRecord {
        self.source
    }

    /// Recovers the exact source record and target topic owner.
    pub fn into_parts(self) -> (OwnedConsumerRecord, Arc<str>) {
        (self.source, self.target_topic)
    }
}

impl fmt::Debug for TransferRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransferRejection")
            .field("source", &self.source)
            .field("target_topic", &self.target_topic)
            .finish()
    }
}

impl fmt::Display for TransferRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("consumer record transfer allocation failed")
    }
}

impl std::error::Error for TransferRejection {}

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
