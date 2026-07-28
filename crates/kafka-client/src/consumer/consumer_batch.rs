//! Linear facade ownership over one bounded group-consumer delivery lease.

use crate::bridge::consumer_facade::group_consumer_batch::GroupConsumerBatch;

use super::{Checkpoint, GroupConsumerRecord, GroupConsumerRecords};

/// Owned group-consumer batch whose record views borrow its retained storage.
///
/// Dropping the batch synchronously returns its exact bounded delivery lease.
/// Consuming it into a checkpoint first releases that lease, then transfers the
/// exact group and assignment fencing required by commit admission.
#[must_use = "dropping the batch releases its bounded group delivery lease"]
pub struct ConsumerBatch {
    inner: GroupConsumerBatch,
}

impl ConsumerBatch {
    pub(crate) const fn from_bridge(inner: GroupConsumerBatch) -> Self {
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

    /// Iterates normalized application records in Kafka order without copying.
    pub fn records(&self) -> GroupConsumerRecords<'_> {
        GroupConsumerRecords::from_bridge(self.inner.records())
    }

    /// Iterates normalized application records in Kafka order without copying.
    pub fn iter(&self) -> GroupConsumerRecords<'_> {
        self.records()
    }

    /// Consumes the whole batch into its exact assignment-fenced checkpoint.
    pub fn checkpoint(self) -> Checkpoint {
        Checkpoint::from_bridge(self.inner.checkpoint())
    }

    /// Compatibility alias for [`Self::checkpoint`].
    pub fn into_checkpoint(self) -> Checkpoint {
        self.checkpoint()
    }
}

impl<'batch> IntoIterator for &'batch ConsumerBatch {
    type Item = GroupConsumerRecord<'batch>;
    type IntoIter = GroupConsumerRecords<'batch>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl std::fmt::Debug for ConsumerBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConsumerBatch")
            .field("topic", &self.topic())
            .field("partition", &self.partition())
            .field("checkpoint_next_offset", &self.checkpoint_next_offset())
            .field("len", &self.len())
            .finish()
    }
}
