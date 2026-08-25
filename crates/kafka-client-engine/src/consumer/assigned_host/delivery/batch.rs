//! Linear public batch ownership over one normalized Fetch delivery.

use std::sync::Arc;

use super::{
    super::state::AssignedConsumerShardState, AssignedConsumerDelivery,
    AssignedConsumerFetchEvidence, AssignedConsumerOwnedBatch, AssignedConsumerOwnedRecords,
    AssignedConsumerRecords,
};

/// Shared final-drop owner for one exact direct-consumer delivery lease.
pub(super) struct AssignedConsumerSharedDelivery {
    delivery: Option<AssignedConsumerDelivery>,
    return_to: Arc<AssignedConsumerShardState>,
}

impl AssignedConsumerSharedDelivery {
    fn new(delivery: AssignedConsumerDelivery, return_to: Arc<AssignedConsumerShardState>) -> Self {
        Self {
            delivery: Some(delivery),
            return_to,
        }
    }

    pub(super) fn delivery(&self) -> &AssignedConsumerDelivery {
        &self.delivery.as_slice()[0]
    }
}

impl Drop for AssignedConsumerSharedDelivery {
    fn drop(&mut self) {
        if let Some(delivery) = self.delivery.take() {
            self.return_to.return_assigned_delivery(delivery);
        }
    }
}

/// One engine-owned direct-consumer delivery and its bounded byte lease.
///
/// Dropping the batch synchronously returns the exact lease to the assigned
/// consumer owner. The owner lock is never held while requesting reactor work.
#[must_use = "dropping the batch returns its bounded delivery lease"]
pub struct AssignedConsumerBatch {
    delivery: Arc<AssignedConsumerSharedDelivery>,
}

impl AssignedConsumerBatch {
    pub(in crate::consumer::assigned_host) fn new(
        delivery: AssignedConsumerDelivery,
        return_to: Arc<AssignedConsumerShardState>,
    ) -> Self {
        Self {
            delivery: Arc::new(AssignedConsumerSharedDelivery::new(delivery, return_to)),
        }
    }

    /// Returns the catalog-owned Kafka topic name.
    pub fn topic(&self) -> &str {
        self.delivery().topic()
    }

    /// Returns the zero-based Kafka partition.
    pub fn partition(&self) -> i32 {
        self.delivery().partition()
    }

    /// Returns the next offset after every record represented by this delivery.
    pub fn checkpoint_next_offset(&self) -> i64 {
        self.delivery().lease().next_offset().get()
    }

    /// Returns one immutable view of this delivery's broker-correlated Fetch facts.
    pub fn evidence(&self) -> AssignedConsumerFetchEvidence {
        AssignedConsumerFetchEvidence::from_delivery(self.delivery())
    }

    /// Iterates normalized application records in Kafka order.
    pub fn records(&self) -> AssignedConsumerRecords<'_> {
        AssignedConsumerRecords::new(self.delivery())
    }

    /// Consumes this delivery into an owned batch retaining its exact lease.
    pub fn into_owned(self) -> AssignedConsumerOwnedBatch {
        AssignedConsumerOwnedBatch::new(self.delivery)
    }

    /// Consumes this batch into non-clone record owners sharing its exact lease.
    ///
    /// The delivery is returned only after the iterator and every record it
    /// yielded have been dropped or transferred together with their bytes.
    pub fn into_owned_records(self) -> AssignedConsumerOwnedRecords {
        self.into_owned().into_records()
    }

    /// Counts normalized application records without copying their bytes.
    pub fn record_count(&self) -> usize {
        self.records().count()
    }

    fn delivery(&self) -> &AssignedConsumerDelivery {
        self.delivery.delivery()
    }
}

impl std::fmt::Debug for AssignedConsumerBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerBatch")
            .field("topic", &self.topic())
            .field("partition", &self.partition())
            .field("checkpoint_next_offset", &self.checkpoint_next_offset())
            .field("record_count", &self.record_count())
            .finish()
    }
}
