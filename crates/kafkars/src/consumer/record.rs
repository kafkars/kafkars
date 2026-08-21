//! Borrowed facade views over one retained assigned-consumer batch.

use crate::bridge::consumer::{
    AssignedConsumerHeader, AssignedConsumerRecord, AssignedConsumerRecords,
};

/// Records borrowing their bytes from one retained [`super::RecordBatch`].
#[derive(Debug)]
pub struct ConsumerRecords<'batch> {
    inner: AssignedConsumerRecords<'batch>,
}

impl<'batch> ConsumerRecords<'batch> {
    pub(super) const fn from_bridge(inner: AssignedConsumerRecords<'batch>) -> Self {
        Self { inner }
    }
}

impl<'batch> Iterator for ConsumerRecords<'batch> {
    type Item = ConsumerRecord<'batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(ConsumerRecord::from_bridge)
    }
}

/// One Kafka record borrowing all variable-length data from its batch.
#[derive(Debug)]
pub struct ConsumerRecord<'batch> {
    inner: AssignedConsumerRecord<'batch>,
}

impl<'batch> ConsumerRecord<'batch> {
    const fn from_bridge(inner: AssignedConsumerRecord<'batch>) -> Self {
        Self { inner }
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
    pub fn headers(&self) -> impl ExactSizeIterator<Item = ConsumerHeader<'_>> {
        self.inner.headers().map(ConsumerHeader::from_bridge)
    }
}

/// One Kafka header borrowing its key and nullable value from a record batch.
#[derive(Debug)]
pub struct ConsumerHeader<'record> {
    inner: AssignedConsumerHeader<'record>,
}

impl<'record> ConsumerHeader<'record> {
    const fn from_bridge(inner: AssignedConsumerHeader<'record>) -> Self {
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
