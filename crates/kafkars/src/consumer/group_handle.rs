//! Unique public ownership of one hosted classic-group registration.

use crate::{
    KafkaError,
    bridge::consumer_facade::group_consumer::GroupConsumerEngine,
    consumer::{ConsumerAssignment, GroupMetadata},
};

/// Unique classic-group consumer with hosted membership and processing liveness.
#[derive(Debug)]
pub struct Consumer {
    pub(super) engine: GroupConsumerEngine,
    pub(super) group_id: String,
    pub(super) topics: Vec<String>,
}

impl Consumer {
    /// Returns the registered Kafka group spelling.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the caller-ordered registered local subscription.
    pub fn subscription(&self) -> &[String] {
        &self.topics
    }

    /// Returns a retained startup fault after accepted membership ownership.
    pub fn startup_error(&self) -> Option<KafkaError> {
        self.engine.startup_fault()
    }

    /// Copies the current Sync-confirmed assignment without consuming events.
    ///
    /// `Ok(None)` means the consumer is joining or its prior assignment is no
    /// longer current.
    pub fn assignment(&self) -> Result<Option<ConsumerAssignment>, KafkaError> {
        self.engine
            .state()
            .map(|state| state.map(|(assignment, _metadata)| assignment))
    }

    /// Copies metadata for the current Sync-confirmed classic membership.
    ///
    /// `Ok(None)` means no membership is currently valid for offset fencing.
    pub fn group_metadata(&self) -> Result<Option<GroupMetadata>, KafkaError> {
        self.engine
            .state()
            .map(|state| state.map(|(_assignment, metadata)| metadata))
    }
}
