//! Unique public ownership of one directly assigned consumer.

use crate::bridge::consumer::AssignedConsumerEngine;

use super::{
    AssignedConsumerEvent, CloseAssignedConsumer, NextAssignedEvent, RecordBatch,
    RecvAssignedBatch, StartPosition, TopicPartition,
};

/// Consumer whose positions are controlled directly rather than by a group.
///
#[derive(Debug)]
pub struct AssignedConsumer {
    pub(super) engine: AssignedConsumerEngine,
}

impl AssignedConsumer {
    pub(crate) const fn new(engine: AssignedConsumerEngine) -> Self {
        Self { engine }
    }

    /// Attempts an immediate all-or-nothing assignment replacement.
    ///
    /// The absolute position-resolution deadline starts before input conversion.
    /// Rejection leaves both this handle and its previously accepted assignment
    /// available for retry.
    pub fn try_replace_assignment<I>(
        &mut self,
        entries: I,
        resolution_timeout: std::time::Duration,
    ) -> Result<(), crate::KafkaError>
    where
        I: IntoIterator<Item = TopicPartition>,
    {
        self.engine
            .try_replace_assignment(entries, resolution_timeout)
    }

    /// Attempts to pause one partition in the active direct assignment.
    ///
    /// Only `partition`'s topic and partition identity are observed. This
    /// operation has no position-resolution deadline.
    pub fn try_pause(&mut self, partition: &TopicPartition) -> Result<(), crate::KafkaError> {
        self.engine.try_pause(partition)
    }

    /// Attempts to resume one partition in the active direct assignment.
    ///
    /// The absolute position-resolution deadline starts before target
    /// conversion. Rejection preserves the unique consumer and assignment.
    pub fn try_resume(
        &mut self,
        partition: &TopicPartition,
        resolution_timeout: std::time::Duration,
    ) -> Result<(), crate::KafkaError> {
        self.engine.try_resume(partition, resolution_timeout)
    }

    /// Attempts to replace one partition's next position.
    ///
    /// The absolute position-resolution deadline starts before target and
    /// position conversion. Rejection preserves the consumer and assignment.
    pub fn try_seek(
        &mut self,
        partition: &TopicPartition,
        position: StartPosition,
        resolution_timeout: std::time::Duration,
    ) -> Result<(), crate::KafkaError> {
        self.engine
            .try_seek(partition, position, resolution_timeout)
    }

    /// Takes one already-authorized prefetched batch when immediately available.
    ///
    /// This call has no application timeout and does not start Fetch work.
    pub fn try_take_batch(&mut self) -> Result<Option<RecordBatch>, crate::KafkaError> {
        self.engine
            .try_take_batch()
            .map(|batch| batch.map(RecordBatch::from_bridge))
    }

    /// Waits for one already-authorized background Fetch delivery.
    ///
    /// This operation creates no application timeout and does not start Fetch.
    pub fn recv(&mut self) -> RecvAssignedBatch<'_> {
        RecvAssignedBatch::from_bridge(self.engine.recv())
    }

    /// Takes one retained failure event when immediately available.
    ///
    /// This call has no timeout, starts no Fetch work, and remains usable while
    /// already-retained events drain after close admission.
    pub fn try_take_event(&mut self) -> Result<Option<AssignedConsumerEvent>, crate::KafkaError> {
        self.engine.try_take_event()
    }

    /// Waits for one already-retained failure event.
    ///
    /// This operation creates no timeout, starts no Fetch work, and can drain
    /// retained events after close admission.
    pub fn next_event(&mut self) -> NextAssignedEvent<'_> {
        NextAssignedEvent::from_bridge(self.engine.next_event())
    }

    /// Attempts to close this consumer and returns the sole terminal observer.
    ///
    /// Close admission reserves its terminal capacity before deterministic core
    /// policy closes later work. Rejection leaves this unique consumer available
    /// for an explicit retry.
    pub fn try_close(&mut self) -> Result<CloseAssignedConsumer, crate::KafkaError> {
        self.engine
            .try_close()
            .map(CloseAssignedConsumer::from_bridge)
    }
}
