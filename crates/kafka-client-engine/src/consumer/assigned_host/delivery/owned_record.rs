//! Non-clone record owners sharing one final-return assigned delivery lease.

use std::sync::Arc;

use bytes::Bytes;

use crate::protocol::fetch::{FetchBatch, FetchRecord};

use super::{
    AssignedConsumerHeader, batch::AssignedConsumerSharedDelivery, record::application_batches,
};

/// Consuming iterator whose yielded records retain the exact delivery lease.
#[must_use = "dropping the iterator releases its share of the delivery lease"]
pub struct AssignedConsumerOwnedRecords {
    delivery: Option<Arc<AssignedConsumerSharedDelivery>>,
    batch_index: usize,
    record_index: usize,
}

impl AssignedConsumerOwnedRecords {
    pub(super) fn new(delivery: Arc<AssignedConsumerSharedDelivery>) -> Self {
        Self {
            delivery: Some(delivery),
            batch_index: 0,
            record_index: 0,
        }
    }
}

impl Iterator for AssignedConsumerOwnedRecords {
    type Item = AssignedConsumerOwnedRecord;

    fn next(&mut self) -> Option<Self::Item> {
        let delivery = self.delivery.as_ref()?;
        let batches = application_batches(delivery.delivery());
        let position = next_position(batches, &mut self.batch_index, &mut self.record_index);
        let Some((batch_index, record_index)) = position else {
            self.delivery = None;
            return None;
        };
        Some(AssignedConsumerOwnedRecord {
            delivery: Arc::clone(delivery),
            batch_index,
            record_index,
        })
    }
}

impl std::fmt::Debug for AssignedConsumerOwnedRecords {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerOwnedRecords")
            .field("batch_index", &self.batch_index)
            .field("record_index", &self.record_index)
            .field("lease_retained", &self.delivery.is_some())
            .finish()
    }
}

/// One non-clone application record retaining its delivery's byte lease.
#[must_use = "dropping the record releases its share of the delivery lease"]
pub struct AssignedConsumerOwnedRecord {
    delivery: Arc<AssignedConsumerSharedDelivery>,
    batch_index: usize,
    record_index: usize,
}

impl AssignedConsumerOwnedRecord {
    /// Returns the retained Kafka topic name.
    pub fn topic(&self) -> &str {
        self.delivery.delivery().topic()
    }

    /// Returns the zero-based Kafka partition.
    pub fn partition(&self) -> i32 {
        self.delivery.delivery().partition()
    }

    /// Returns the absolute Kafka log offset.
    pub fn offset(&self) -> i64 {
        self.record().offset
    }

    /// Returns the Kafka timestamp in milliseconds when present.
    pub fn timestamp_millis(&self) -> Option<i64> {
        self.record().timestamp
    }

    /// Returns the nullable key without exposing a clonable byte owner.
    pub fn key(&self) -> Option<&[u8]> {
        self.record().key.as_deref()
    }

    /// Returns the nullable value without exposing a clonable byte owner.
    pub fn value(&self) -> Option<&[u8]> {
        self.record().value.as_deref()
    }

    /// Returns duplicate-preserving borrowed headers in broker order.
    pub fn headers(&self) -> impl ExactSizeIterator<Item = AssignedConsumerHeader<'_>> {
        self.record()
            .headers
            .iter()
            .map(AssignedConsumerHeader::from_fetch)
    }

    /// Clones the nullable key's shared byte handle without copying its contents.
    #[doc(hidden)]
    pub fn shared_key(&self) -> Option<Bytes> {
        self.record().key.clone()
    }

    /// Clones the nullable value's shared byte handle without copying its contents.
    #[doc(hidden)]
    pub fn shared_value(&self) -> Option<Bytes> {
        self.record().value.clone()
    }

    /// Clones the exact delivery owner for a facade record built from shared bytes.
    ///
    /// The facade must attach this owner to every public byte owner derived from
    /// this record until it has committed replacement retained-byte accounting.
    #[doc(hidden)]
    pub fn shared_source_owner(&self) -> Arc<dyn Send + Sync> {
        Arc::clone(&self.delivery) as Arc<dyn Send + Sync>
    }

    fn record(&self) -> &FetchRecord {
        &application_batches(self.delivery.delivery())[self.batch_index].records[self.record_index]
    }
}

impl std::fmt::Debug for AssignedConsumerOwnedRecord {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerOwnedRecord")
            .field("topic", &self.topic())
            .field("partition", &self.partition())
            .field("offset", &self.offset())
            .field("timestamp_millis", &self.timestamp_millis())
            .field("header_count", &self.record().headers.len())
            .finish_non_exhaustive()
    }
}

fn next_position(
    batches: &[FetchBatch],
    batch_index: &mut usize,
    record_index: &mut usize,
) -> Option<(usize, usize)> {
    loop {
        let batch = batches.get(*batch_index)?;
        if batch.is_control {
            *batch_index += 1;
            *record_index = 0;
            continue;
        }
        if batch.records.get(*record_index).is_some() {
            let position = (*batch_index, *record_index);
            *record_index += 1;
            return Some(position);
        }
        *batch_index += 1;
        *record_index = 0;
    }
}
