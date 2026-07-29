//! Stable caller-owned identity for aborting one partition transaction.

use crate::TopicPartition;

/// Exact producer and coordinator identity for one partition transaction abort.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AbortTransactionSpec {
    topic_partition: TopicPartition,
    producer_id: i64,
    producer_epoch: i16,
    coordinator_epoch: i32,
}

impl AbortTransactionSpec {
    /// Creates one inert partition-transaction abort specification.
    ///
    /// Validation is deferred until [`super::AbortTransactionBuilder::submit`]
    /// so the public deadline covers every fallible preparation step.
    pub const fn new(
        topic_partition: TopicPartition,
        producer_id: i64,
        producer_epoch: i16,
        coordinator_epoch: i32,
    ) -> Self {
        Self {
            topic_partition,
            producer_id,
            producer_epoch,
            coordinator_epoch,
        }
    }

    /// Returns the target topic-partition.
    pub const fn topic_partition(&self) -> &TopicPartition {
        &self.topic_partition
    }

    /// Returns Kafka's signed producer ID.
    pub const fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Returns Kafka's signed producer epoch.
    pub const fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    /// Returns the transaction coordinator epoch that authorized the marker.
    pub const fn coordinator_epoch(&self) -> i32 {
        self.coordinator_epoch
    }

    pub(crate) fn into_parts(self) -> (TopicPartition, i64, i16, i32) {
        (
            self.topic_partition,
            self.producer_id,
            self.producer_epoch,
            self.coordinator_epoch,
        )
    }
}
