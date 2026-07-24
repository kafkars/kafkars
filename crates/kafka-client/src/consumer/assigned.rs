//! Unique public ownership of one directly assigned consumer.

use crate::bridge::consumer::AssignedConsumerEngine;

use super::{CloseAssignedConsumer, TopicPartition};

/// Consumer whose positions are controlled directly rather than by a group.
///
/// Record delivery remains absent until its engine seam is complete.
#[derive(Debug)]
pub struct AssignedConsumer {
    engine: AssignedConsumerEngine,
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
