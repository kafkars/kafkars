//! Linear owned batch capability retaining one exact assigned delivery lease.

use std::sync::Arc;

use super::{AssignedConsumerOwnedRecords, batch::AssignedConsumerSharedDelivery};

/// One owned direct-consumer batch whose derived records share its byte lease.
#[must_use = "dropping the batch releases its share of the delivery lease"]
pub struct AssignedConsumerOwnedBatch {
    delivery: Arc<AssignedConsumerSharedDelivery>,
}

impl AssignedConsumerOwnedBatch {
    pub(super) fn new(delivery: Arc<AssignedConsumerSharedDelivery>) -> Self {
        Self { delivery }
    }

    /// Returns the retained Kafka topic name.
    pub fn topic(&self) -> &str {
        self.delivery.delivery().topic()
    }

    /// Returns the zero-based Kafka partition.
    pub fn partition(&self) -> i32 {
        self.delivery.delivery().partition()
    }

    /// Returns the next offset after every record represented by this batch.
    pub fn checkpoint_next_offset(&self) -> i64 {
        self.delivery.delivery().lease().next_offset().get()
    }

    /// Returns the number of normalized application records.
    pub fn record_count(&self) -> usize {
        super::record::application_batches(self.delivery.delivery())
            .iter()
            .filter(|batch| !batch.is_control)
            .map(|batch| batch.records.len())
            .sum()
    }

    /// Consumes the batch into a bounded iterator of non-clone record owners.
    pub fn into_records(self) -> AssignedConsumerOwnedRecords {
        AssignedConsumerOwnedRecords::new(self.delivery)
    }
}

impl std::fmt::Debug for AssignedConsumerOwnedBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerOwnedBatch")
            .field("topic", &self.topic())
            .field("partition", &self.partition())
            .field("checkpoint_next_offset", &self.checkpoint_next_offset())
            .field("record_count", &self.record_count())
            .finish()
    }
}
