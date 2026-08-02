//! Immediate Fetch control and clone-shared shutdown for one hosted group.

use crate::{KafkaError, bridge::consumer_facade::group_consumer_control::GroupConsumerControl};

use super::{Consumer, TopicPartition};

/// Thread-safe control operations that do not mutate subscription state.
#[derive(Debug, Clone)]
pub struct ConsumerControl {
    inner: GroupConsumerControl,
}

impl Consumer {
    /// Clones a thread-safe capability for requesting group shutdown.
    pub fn control(&self) -> ConsumerControl {
        ConsumerControl::from_bridge(self.engine.control())
    }

    /// Pauses Fetch progress for every named partition in the current assignment.
    ///
    /// The complete borrowed slice is copied and validated before one immediate
    /// control admission. An empty slice is an inert success.
    pub fn pause(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        self.engine.pause(partitions)
    }

    /// Resumes Fetch progress for every named partition in the current assignment.
    ///
    /// The complete borrowed slice is copied and validated under one capture-first
    /// resume boundary before deterministic admission. An empty slice is inert.
    pub fn resume(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        self.engine.resume(partitions)
    }
}

impl ConsumerControl {
    pub(crate) const fn from_bridge(inner: GroupConsumerControl) -> Self {
        Self { inner }
    }

    /// Requests idempotent shutdown of this exact registered group.
    ///
    /// The group host retains the first request boundary and continues the
    /// existing broker leave even if the unique [`Consumer`] is later dropped.
    pub fn request_shutdown(&self) {
        self.inner.request_shutdown();
    }
}
