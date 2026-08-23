//! Borrowed facade views over one retained share-consumer delivery.

use crate::bridge::share_consumer::{
    ShareConsumerHeader as BridgeHeader, ShareConsumerRecord as BridgeRecord,
    ShareConsumerRecords as BridgeRecords,
};

use super::{ShareDisposition, ShareRecordDecision};

/// Records borrowing their bytes from one retained [`super::ShareConsumerBatch`].
#[derive(Debug)]
pub struct ShareConsumerRecords<'batch> {
    inner: BridgeRecords<'batch>,
}

impl<'batch> ShareConsumerRecords<'batch> {
    pub(super) const fn from_bridge(inner: BridgeRecords<'batch>) -> Self {
        Self { inner }
    }
}

impl<'batch> Iterator for ShareConsumerRecords<'batch> {
    type Item = ShareConsumerRecord<'batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(ShareConsumerRecord::from_bridge)
    }
}

/// One acquired Kafka record borrowing data from its exact share batch.
#[derive(Debug)]
pub struct ShareConsumerRecord<'batch> {
    inner: BridgeRecord<'batch>,
}

impl<'batch> ShareConsumerRecord<'batch> {
    const fn from_bridge(inner: BridgeRecord<'batch>) -> Self {
        Self { inner }
    }

    /// Returns the retained Kafka topic name.
    pub fn topic(&self) -> &str {
        self.inner.topic()
    }

    /// Returns the zero-based Kafka partition.
    pub const fn partition(&self) -> u32 {
        self.inner.partition()
    }

    /// Returns the absolute Kafka log offset.
    pub const fn offset(&self) -> i64 {
        self.inner.offset()
    }

    /// Returns Kafka's positive delivery count for this acquired range.
    pub const fn delivery_count(&self) -> i16 {
        self.inner.delivery_count()
    }

    /// Correlates one disposition to this exact acquired record.
    pub const fn decision(&self, disposition: ShareDisposition) -> ShareRecordDecision {
        ShareRecordDecision::from_bridge(self.inner.decision(disposition.into_bridge()))
    }

    /// Returns the Kafka timestamp in milliseconds when present.
    pub const fn timestamp_millis(&self) -> Option<i64> {
        self.inner.timestamp_millis()
    }

    /// Returns the nullable key without conflating null and empty bytes.
    pub fn key(&self) -> Option<&[u8]> {
        self.inner.key()
    }

    /// Returns the nullable value without conflating tombstones and empty bytes.
    pub fn value(&self) -> Option<&[u8]> {
        self.inner.value()
    }

    /// Returns duplicate-preserving borrowed headers in broker order.
    pub fn headers(&self) -> impl ExactSizeIterator<Item = ShareConsumerHeader<'_>> {
        self.inner.headers().map(ShareConsumerHeader::from_bridge)
    }
}

/// One Kafka header borrowing its key and nullable value from a share batch.
#[derive(Debug)]
pub struct ShareConsumerHeader<'record> {
    inner: BridgeHeader<'record>,
}

impl<'record> ShareConsumerHeader<'record> {
    const fn from_bridge(inner: BridgeHeader<'record>) -> Self {
        Self { inner }
    }

    /// Returns the header key bytes.
    pub fn key(&self) -> &[u8] {
        self.inner.key()
    }

    /// Returns the nullable value without conflating null and empty bytes.
    pub fn value(&self) -> Option<&[u8]> {
        self.inner.value()
    }
}
