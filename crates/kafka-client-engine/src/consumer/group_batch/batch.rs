//! Linear public ownership of one assignment-fenced classic-group delivery.

use std::sync::Arc;

use super::{GroupConsumerCheckpoint, GroupConsumerRecords};
use crate::consumer::{
    group::{ClassicGroupFetchDelivery, GroupConsumerPort},
    group_batch::checkpoint::checkpoint_from_delivery,
};

/// One engine-owned classic-group delivery and its bounded byte lease.
///
/// Dropping the batch synchronously returns its exact lease to the group Fetch
/// owner. The registry lock is released before reactor work is requested.
#[must_use = "dropping the batch returns its bounded group delivery lease"]
pub struct GroupConsumerBatch {
    delivery: Option<ClassicGroupFetchDelivery>,
    checkpoint: Option<GroupConsumerCheckpoint>,
    return_to: GroupConsumerPort,
    _lifetime: Arc<dyn Send + Sync>,
}

impl GroupConsumerBatch {
    pub(in crate::consumer) fn new(
        delivery: ClassicGroupFetchDelivery,
        return_to: GroupConsumerPort,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Self {
        let checkpoint = checkpoint_from_delivery(&delivery);
        Self {
            delivery: Some(delivery),
            checkpoint: Some(checkpoint),
            return_to,
            _lifetime: lifetime,
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
        self.delivery().next_offset().get()
    }

    /// Iterates normalized application records in Kafka order.
    pub fn records(&self) -> GroupConsumerRecords<'_> {
        GroupConsumerRecords::new(self.delivery())
    }

    /// Counts normalized application records without copying their bytes.
    pub fn record_count(&self) -> usize {
        self.records().count()
    }

    /// Consumes this batch and returns its exact assignment-fenced checkpoint.
    ///
    /// Consuming the batch first returns its bounded delivery lease. The
    /// checkpoint remains independently owned for a later commit admission.
    pub fn checkpoint(mut self) -> GroupConsumerCheckpoint {
        self.checkpoint
            .take()
            .unwrap_or_else(|| unreachable!("live batch retains one checkpoint"))
    }

    /// Compatibility alias for [`Self::checkpoint`].
    pub fn into_checkpoint(self) -> GroupConsumerCheckpoint {
        self.checkpoint()
    }

    fn delivery(&self) -> &ClassicGroupFetchDelivery {
        self.delivery
            .as_slice()
            .first()
            .unwrap_or_else(|| unreachable!("public batch methods cannot run after Drop"))
    }
}

impl Drop for GroupConsumerBatch {
    fn drop(&mut self) {
        if let Some(delivery) = self.delivery.take() {
            self.return_to.return_delivery_blocking(delivery);
        }
    }
}

impl std::fmt::Debug for GroupConsumerBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GroupConsumerBatch")
            .field("topic", &self.topic())
            .field("partition", &self.partition())
            .field("checkpoint_next_offset", &self.checkpoint_next_offset())
            .field("record_count", &self.record_count())
            .finish()
    }
}
