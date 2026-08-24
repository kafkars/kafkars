//! Linear owned batch capability for directly assigned consumer deliveries.

use crate::bridge::consumer as bridge;

use super::OwnedConsumerRecords;

/// One owned direct-consumer batch retaining its exact delivery lease.
#[must_use = "dropping the batch releases its bounded delivery lease"]
#[derive(Debug)]
pub struct OwnedConsumerBatch {
    inner: bridge::AssignedConsumerOwnedBatch,
}

impl OwnedConsumerBatch {
    pub(super) const fn from_bridge(inner: bridge::AssignedConsumerOwnedBatch) -> Self {
        Self { inner }
    }

    /// Returns the retained Kafka topic name.
    pub fn topic(&self) -> &str {
        self.inner.topic()
    }

    /// Returns the zero-based source partition.
    pub fn partition(&self) -> i32 {
        self.inner.partition()
    }

    /// Returns the next offset after every record represented by this batch.
    pub fn checkpoint_next_offset(&self) -> i64 {
        self.inner.checkpoint_next_offset()
    }

    /// Returns the number of normalized application records.
    pub fn len(&self) -> usize {
        self.inner.record_count()
    }

    /// Returns whether this batch contains no application records.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Consumes the batch into a bounded iterator of non-clone record owners.
    pub fn into_records(self) -> OwnedConsumerRecords {
        OwnedConsumerRecords::from_bridge(self.inner.into_records())
    }
}
