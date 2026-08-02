//! Borrowed facade views over one retained group-consumer batch.

use crate::bridge::consumer_facade::group_consumer_batch::{
    GroupConsumerHeader as BridgeHeader, GroupConsumerRecord as BridgeRecord,
    GroupConsumerRecords as BridgeRecords,
};

/// Records borrowing their bytes from one retained [`super::ConsumerBatch`].
#[derive(Debug)]
pub struct GroupConsumerRecords<'batch> {
    inner: BridgeRecords<'batch>,
}

impl<'batch> GroupConsumerRecords<'batch> {
    pub(super) const fn from_bridge(inner: BridgeRecords<'batch>) -> Self {
        Self { inner }
    }
}

impl<'batch> Iterator for GroupConsumerRecords<'batch> {
    type Item = GroupConsumerRecord<'batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(GroupConsumerRecord::from_bridge)
    }
}

/// One Kafka record borrowing all variable-length data from a consumer batch.
#[derive(Debug)]
pub struct GroupConsumerRecord<'batch> {
    inner: BridgeRecord<'batch>,
}

impl<'batch> GroupConsumerRecord<'batch> {
    const fn from_bridge(inner: BridgeRecord<'batch>) -> Self {
        Self { inner }
    }

    pub(super) const fn as_bridge(&self) -> &BridgeRecord<'batch> {
        &self.inner
    }

    /// Returns the retained Kafka topic name.
    pub fn topic(&self) -> &str {
        self.inner.topic()
    }

    /// Returns the zero-based Kafka partition.
    pub fn partition(&self) -> i32 {
        self.inner.partition()
    }

    /// Returns the absolute Kafka log offset.
    pub fn offset(&self) -> i64 {
        self.inner.offset()
    }

    /// Returns the Kafka timestamp in milliseconds when present.
    pub fn timestamp_millis(&self) -> Option<i64> {
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
    pub fn headers(&self) -> impl ExactSizeIterator<Item = GroupConsumerHeader<'_>> {
        self.inner.headers().map(GroupConsumerHeader::from_bridge)
    }
}

/// One Kafka header borrowing its key and nullable value from a consumer batch.
#[derive(Debug)]
pub struct GroupConsumerHeader<'record> {
    inner: BridgeHeader<'record>,
}

impl<'record> GroupConsumerHeader<'record> {
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
