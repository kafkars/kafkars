//! Unique public ownership of one hosted Kafka share-group member.

use crate::{KafkaError, bridge::share_consumer::ShareConsumerEngine};

use super::ShareConsumerAssignment;

/// Unique share consumer with hosted membership and no implicit acknowledgements.
#[derive(Debug)]
pub struct ShareConsumer {
    pub(super) engine: ShareConsumerEngine,
    pub(super) group_id: String,
    pub(super) rack: Option<String>,
    pub(super) topics: Vec<String>,
}

impl ShareConsumer {
    pub(crate) const fn new(
        engine: ShareConsumerEngine,
        group_id: String,
        rack: Option<String>,
        topics: Vec<String>,
    ) -> Self {
        Self {
            engine,
            group_id,
            rack,
            topics,
        }
    }

    /// Returns the registered Kafka share-group spelling.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the caller-ordered registered topic subscription.
    pub fn subscription(&self) -> &[String] {
        &self.topics
    }

    /// Returns the optional rack spelling sent during membership.
    pub fn rack(&self) -> Option<&str> {
        self.rack.as_deref()
    }

    /// Returns a retained terminal if the first share heartbeat never succeeded.
    pub fn startup_error(&self) -> Option<KafkaError> {
        self.engine.startup_fault()
    }

    /// Copies the current broker-confirmed membership and assignment.
    ///
    /// `Ok(None)` means the member is joining or has no current assignment.
    pub fn assignment(&self) -> Result<Option<ShareConsumerAssignment>, KafkaError> {
        self.engine.state()
    }
}
