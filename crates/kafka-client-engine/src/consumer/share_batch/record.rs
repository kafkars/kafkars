//! Borrowed flattened views over normalized share-consumer records.

use kafka_client_core::{ShareAcquisition, ShareDisposition, ShareRecordDecision};

use crate::{
    consumer::share::{ShareFetchDelivery, ShareFetchDeliveryPartition},
    protocol::fetch::{FetchHeader, FetchRecord},
};

/// Ordered borrowed application records from one response-wide share delivery.
pub struct ShareConsumerRecords<'batch> {
    delivery: &'batch ShareFetchDelivery,
    partition_index: usize,
    batch_index: usize,
    record_index: usize,
    next_ordinal: usize,
}

impl<'batch> ShareConsumerRecords<'batch> {
    pub(super) const fn new(delivery: &'batch ShareFetchDelivery) -> Self {
        Self {
            delivery,
            partition_index: 0,
            batch_index: 0,
            record_index: 0,
            next_ordinal: 0,
        }
    }
}

impl<'batch> Iterator for ShareConsumerRecords<'batch> {
    type Item = ShareConsumerRecord<'batch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let partition = self.delivery.partitions().get(self.partition_index)?;
            let Some(batch) = partition.batches().get(self.batch_index) else {
                self.partition_index += 1;
                self.batch_index = 0;
                self.record_index = 0;
                continue;
            };
            if batch.is_control {
                self.batch_index += 1;
                self.record_index = 0;
                continue;
            }
            let Some(record) = batch.records.get(self.record_index) else {
                self.batch_index += 1;
                self.record_index = 0;
                continue;
            };
            self.record_index += 1;
            let ordinal = self.next_ordinal;
            self.next_ordinal = self.next_ordinal.saturating_add(1);
            let acquisition = acquisition_for(self.delivery, partition, record.offset)
                .unwrap_or_else(|| unreachable!("decoded share records retain acquisition facts"));
            return Some(ShareConsumerRecord {
                delivery: self.delivery,
                partition,
                acquisition,
                ordinal,
                record,
            });
        }
    }
}

fn acquisition_for<'delivery>(
    delivery: &'delivery ShareFetchDelivery,
    partition: &ShareFetchDeliveryPartition,
    offset: i64,
) -> Option<&'delivery ShareAcquisition> {
    delivery.acquisitions().iter().find(|acquisition| {
        let range = acquisition.range();
        range.topic_uuid() == partition.topic_uuid()
            && range.partition() == partition.partition()
            && (range.first_offset()..=range.last_offset()).contains(&offset)
    })
}

/// One record borrowing bytes and acquisition identity from its owning batch.
pub struct ShareConsumerRecord<'batch> {
    delivery: &'batch ShareFetchDelivery,
    partition: &'batch ShareFetchDeliveryPartition,
    acquisition: &'batch ShareAcquisition,
    ordinal: usize,
    record: &'batch FetchRecord,
}

impl ShareConsumerRecord<'_> {
    /// Returns the retained Kafka topic name.
    pub fn topic(&self) -> &str {
        self.partition.topic()
    }

    /// Returns the zero-based Kafka partition.
    pub const fn partition(&self) -> u32 {
        self.partition.partition().partition().get()
    }

    /// Returns the absolute Kafka log offset.
    pub const fn offset(&self) -> i64 {
        self.record.offset
    }

    /// Returns Kafka's positive delivery count for this acquired range.
    pub const fn delivery_count(&self) -> i16 {
        self.acquisition.range().delivery_count().get()
    }

    /// Correlates one public disposition to this exact acquired record.
    pub const fn decision(&self, disposition: ShareDisposition) -> ShareRecordDecision {
        ShareRecordDecision::new(
            self.acquisition.generation(),
            self.record.offset,
            disposition,
        )
    }

    pub(super) fn belongs_to(&self, delivery: &ShareFetchDelivery) -> bool {
        std::ptr::eq(self.delivery, delivery)
    }

    pub(super) const fn ordinal(&self) -> usize {
        self.ordinal
    }

    pub(super) const fn acquisition(&self) -> &ShareAcquisition {
        self.acquisition
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
    pub fn headers(&self) -> impl ExactSizeIterator<Item = ShareConsumerHeader<'_>> {
        self.record
            .headers
            .iter()
            .map(|header| ShareConsumerHeader { header })
    }
}

impl std::fmt::Debug for ShareConsumerRecords<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareConsumerRecords")
            .field("partition_index", &self.partition_index)
            .field("batch_index", &self.batch_index)
            .field("record_index", &self.record_index)
            .field("next_ordinal", &self.next_ordinal)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ShareConsumerRecord<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareConsumerRecord")
            .field("topic", &self.topic())
            .field("partition", &self.partition())
            .field("ordinal", &self.ordinal)
            .field("delivery_count", &self.delivery_count())
            .field("record", &self.record)
            .finish()
    }
}

/// One borrowed Kafka header retaining null-versus-empty value semantics.
#[derive(Debug)]
pub struct ShareConsumerHeader<'record> {
    header: &'record FetchHeader,
}

impl ShareConsumerHeader<'_> {
    /// Returns the header key bytes.
    pub fn key(&self) -> &[u8] {
        &self.header.key
    }

    /// Returns the nullable header value bytes.
    pub fn value(&self) -> Option<&[u8]> {
        self.header.value.as_deref()
    }
}
