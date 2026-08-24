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

/// Shared record fields transferred together with their exact delivery owner.
#[doc(hidden)]
#[must_use = "dropping the parts releases the transferred delivery lease"]
pub struct AssignedConsumerOwnedRecordParts {
    /// Kafka timestamp in milliseconds when present.
    pub timestamp_millis: Option<i64>,
    /// Nullable shared key bytes.
    pub key: Option<Bytes>,
    /// Nullable shared value bytes.
    pub value: Option<Bytes>,
    /// Ordered, duplicate-preserving shared headers.
    pub headers: Vec<AssignedConsumerOwnedHeader>,
    /// Opaque owner that returns the exact delivery lease on its final drop.
    pub source_owner: Arc<dyn Send + Sync>,
}

impl std::fmt::Debug for AssignedConsumerOwnedRecordParts {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerOwnedRecordParts")
            .field("timestamp_millis", &self.timestamp_millis)
            .field("key", &self.key)
            .field("value", &self.value)
            .field("headers", &self.headers)
            .field("source_owner", &"<retained>")
            .finish()
    }
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

    /// Transfers shared byte handles together with the exact delivery owner.
    ///
    /// This is an adapter seam for the curated facade. The returned owner must
    /// remain attached to any record built from the byte handles.
    #[doc(hidden)]
    pub fn into_shared_parts(self) -> AssignedConsumerOwnedRecordParts {
        let record = self.record();
        let timestamp_millis = record.timestamp;
        let key = record.key.clone();
        let value = record.value.clone();
        let headers = record
            .headers
            .iter()
            .map(|header| AssignedConsumerOwnedHeader {
                key: header.key.clone(),
                value: header.value.clone(),
            })
            .collect();
        let source_owner: Arc<dyn Send + Sync> = self.delivery;
        AssignedConsumerOwnedRecordParts {
            timestamp_millis,
            key,
            value,
            headers,
            source_owner,
        }
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

/// One header transferred together with an owned record's source lease.
#[doc(hidden)]
#[derive(Debug)]
pub struct AssignedConsumerOwnedHeader {
    key: Bytes,
    value: Option<Bytes>,
}

impl AssignedConsumerOwnedHeader {
    /// Moves the validated key bytes and nullable value into a facade record.
    #[doc(hidden)]
    pub fn into_shared_parts(self) -> (Bytes, Option<Bytes>) {
        (self.key, self.value)
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
