//! Borrowed flattened views over normalized application record descriptors.

use crate::protocol::fetch::{FetchBatch, FetchHeader, FetchRecord};

use super::AssignedConsumerDelivery;

/// Ordered borrowed application records from one direct-consumer delivery.
#[derive(Debug)]
pub struct AssignedConsumerRecords<'batch> {
    topic: &'batch str,
    partition: i32,
    batches: &'batch [FetchBatch],
    batch_index: usize,
    record_index: usize,
}

impl<'batch> AssignedConsumerRecords<'batch> {
    pub(super) fn new(delivery: &'batch AssignedConsumerDelivery) -> Self {
        let batches = delivery
            .lease()
            .outcome()
            .outcome()
            .data_batches()
            .unwrap_or_default();
        Self {
            topic: delivery.topic(),
            partition: delivery.partition(),
            batches,
            batch_index: 0,
            record_index: 0,
        }
    }
}

impl<'batch> Iterator for AssignedConsumerRecords<'batch> {
    type Item = AssignedConsumerRecord<'batch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let batch = self.batches.get(self.batch_index)?;
            if batch.is_control {
                self.batch_index += 1;
                self.record_index = 0;
                continue;
            }
            if let Some(record) = batch.records.get(self.record_index) {
                self.record_index += 1;
                return Some(AssignedConsumerRecord {
                    topic: self.topic,
                    partition: self.partition,
                    record,
                });
            }
            self.batch_index += 1;
            self.record_index = 0;
        }
    }
}

/// One record borrowing bytes from its owning assigned-consumer batch.
#[derive(Debug)]
pub struct AssignedConsumerRecord<'batch> {
    topic: &'batch str,
    partition: i32,
    record: &'batch FetchRecord,
}

impl AssignedConsumerRecord<'_> {
    /// Returns the retained Kafka topic name.
    pub const fn topic(&self) -> &str {
        self.topic
    }

    /// Returns the zero-based Kafka partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the absolute Kafka log offset.
    pub const fn offset(&self) -> i64 {
        self.record.offset
    }

    /// Returns the Kafka timestamp in milliseconds when present.
    pub const fn timestamp_millis(&self) -> Option<i64> {
        self.record.timestamp
    }

    /// Returns the nullable key without conflating null and empty bytes.
    pub fn key(&self) -> Option<&[u8]> {
        self.record.key.as_deref()
    }

    /// Returns the nullable value without conflating tombstones and empty bytes.
    pub fn value(&self) -> Option<&[u8]> {
        self.record.value.as_deref()
    }

    /// Returns duplicate-preserving headers in broker order.
    pub fn headers(&self) -> impl ExactSizeIterator<Item = AssignedConsumerHeader<'_>> {
        self.record
            .headers
            .iter()
            .map(|header| AssignedConsumerHeader { header })
    }
}

/// One borrowed Kafka header retaining null-versus-empty value semantics.
#[derive(Debug)]
pub struct AssignedConsumerHeader<'record> {
    header: &'record FetchHeader,
}

impl AssignedConsumerHeader<'_> {
    /// Returns the header key bytes.
    pub fn key(&self) -> &[u8] {
        &self.header.key
    }

    /// Returns the nullable header value bytes.
    pub fn value(&self) -> Option<&[u8]> {
        self.header.value.as_deref()
    }
}
