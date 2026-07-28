//! Immediate atomic Fetch control for one hosted classic-group assignment.

use crate::KafkaError;

use super::{Consumer, TopicPartition};

impl Consumer {
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
