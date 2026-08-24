//! Incremental direct-assignment admission without disturbing survivor positions.

use std::time::Duration;

use super::{AssignedConsumer, TopicPartition};

impl AssignedConsumer {
    /// Attempts an immediate all-or-nothing addition to the active assignment.
    ///
    /// Every nonempty entry requires an explicit start position. The absolute
    /// position-resolution deadline starts before input iteration or conversion.
    /// Acceptance preserves the exact fences and positions of every partition
    /// that was already assigned; rejection preserves the complete assignment.
    /// An empty addition is inert while this consumer remains open.
    pub fn try_add_assignments<I>(
        &mut self,
        entries: I,
        resolution_timeout: Duration,
    ) -> Result<(), crate::KafkaError>
    where
        I: IntoIterator<Item = TopicPartition>,
    {
        self.engine.try_add_assignments(entries, resolution_timeout)
    }

    /// Attempts an immediate all-or-nothing removal from the active assignment.
    ///
    /// Only each entry's topic and partition identity are observed. This
    /// operation has no position-resolution deadline. Acceptance fences only
    /// removed partitions and leaves every surviving fence and position exact;
    /// rejection preserves the complete assignment. An empty removal is inert
    /// while this consumer remains open.
    pub fn try_remove_assignments<I>(&mut self, entries: I) -> Result<(), crate::KafkaError>
    where
        I: IntoIterator<Item = TopicPartition>,
    {
        self.engine.try_remove_assignments(entries)
    }
}
