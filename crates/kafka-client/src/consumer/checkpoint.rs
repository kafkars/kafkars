//! Exact facade ownership of an assignment-fenced group-consumer checkpoint.

use crate::bridge::consumer_facade::group_consumer_checkpoint::GroupConsumerCheckpoint;

/// Linear next-offset checkpoint for one processed group-consumer batch.
///
/// The private engine owner retains the exact group, member, and assignment
/// identity required to reject stale commits. This facade intentionally
/// exposes only stable topic-partition and next-offset observation.
#[must_use = "a checkpoint should be committed or deliberately discarded"]
pub struct Checkpoint {
    inner: GroupConsumerCheckpoint,
}

impl Checkpoint {
    pub(crate) const fn from_bridge(inner: GroupConsumerCheckpoint) -> Self {
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

    /// Returns the next offset to consume.
    pub fn next_offset(&self) -> i64 {
        self.inner.next_offset()
    }
}

impl std::fmt::Debug for Checkpoint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Checkpoint")
            .field("topic", &self.topic())
            .field("partition", &self.partition())
            .field("next_offset", &self.next_offset())
            .finish_non_exhaustive()
    }
}
