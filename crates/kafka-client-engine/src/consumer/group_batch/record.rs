//! Borrowed flattened views over normalized group Fetch records.

use crate::{
    consumer::group::ClassicGroupFetchDelivery,
    protocol::fetch::{FetchBatch, FetchHeader, FetchRecord},
};

/// Ordered borrowed application records from one classic-group delivery.
pub struct GroupConsumerRecords<'batch> {
    delivery: &'batch ClassicGroupFetchDelivery,
    topic: &'batch str,
    partition: i32,
    batches: &'batch [FetchBatch],
    batch_index: usize,
    record_index: usize,
    next_ordinal: usize,
}

impl<'batch> GroupConsumerRecords<'batch> {
    pub(super) fn new(delivery: &'batch ClassicGroupFetchDelivery) -> Self {
        Self {
            delivery,
            topic: delivery.topic(),
            partition: delivery.partition(),
            batches: delivery.data_batches(),
            batch_index: 0,
            record_index: 0,
            next_ordinal: 0,
        }
    }
}

impl<'batch> Iterator for GroupConsumerRecords<'batch> {
    type Item = GroupConsumerRecord<'batch>;

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
                let ordinal = self.next_ordinal;
                self.next_ordinal = self.next_ordinal.saturating_add(1);
                return Some(GroupConsumerRecord {
                    delivery: self.delivery,
                    topic: self.topic,
                    partition: self.partition,
                    ordinal,
                    record,
                });
            }
            self.batch_index += 1;
            self.record_index = 0;
        }
    }
}

/// One record borrowing bytes from its owning classic-group batch.
pub struct GroupConsumerRecord<'batch> {
    delivery: &'batch ClassicGroupFetchDelivery,
    topic: &'batch str,
    partition: i32,
    ordinal: usize,
    record: &'batch FetchRecord,
}

impl GroupConsumerRecord<'_> {
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

    pub(super) fn belongs_to(&self, delivery: &ClassicGroupFetchDelivery) -> bool {
        std::ptr::eq(self.delivery, delivery)
    }

    pub(super) const fn ordinal(&self) -> usize {
        self.ordinal
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
    pub fn headers(&self) -> impl ExactSizeIterator<Item = GroupConsumerHeader<'_>> {
        self.record
            .headers
            .iter()
            .map(|header| GroupConsumerHeader { header })
    }
}

impl std::fmt::Debug for GroupConsumerRecords<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GroupConsumerRecords")
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .field("batch_index", &self.batch_index)
            .field("record_index", &self.record_index)
            .field("next_ordinal", &self.next_ordinal)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for GroupConsumerRecord<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GroupConsumerRecord")
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .field("ordinal", &self.ordinal)
            .field("record", &self.record)
            .finish()
    }
}

/// One borrowed Kafka header retaining null-versus-empty value semantics.
#[derive(Debug)]
pub struct GroupConsumerHeader<'record> {
    header: &'record FetchHeader,
}

impl GroupConsumerHeader<'_> {
    /// Returns the header key bytes.
    pub fn key(&self) -> &[u8] {
        &self.header.key
    }

    /// Returns the nullable header value bytes.
    pub fn value(&self) -> Option<&[u8]> {
        self.header.value.as_deref()
    }
}
