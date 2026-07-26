//! Stable inert intent for one consumer-group committed-offset alteration.

/// One caller-ordered topic-partition committed-offset alteration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerGroupOffsetAlteration {
    topic: String,
    partition: i32,
    next_offset: i64,
    leader_epoch: Option<i32>,
    metadata: Option<String>,
}

impl ConsumerGroupOffsetAlteration {
    /// Creates one alteration with no leader epoch and null Kafka metadata.
    pub fn new(topic: impl Into<String>, partition: i32, next_offset: i64) -> Self {
        Self {
            topic: topic.into(),
            partition,
            next_offset,
            leader_epoch: None,
            metadata: None,
        }
    }

    /// Supplies the nonnegative leader epoch committed beside the next offset.
    #[must_use]
    pub const fn leader_epoch(mut self, leader_epoch: i32) -> Self {
        self.leader_epoch = Some(leader_epoch);
        self
    }

    /// Supplies Kafka metadata, preserving an explicitly empty string.
    #[must_use]
    pub fn metadata(mut self, metadata: impl Into<String>) -> Self {
        self.metadata = Some(metadata.into());
        self
    }

    /// Returns the requested topic.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the requested partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the committed next offset.
    pub const fn next_offset(&self) -> i64 {
        self.next_offset
    }

    /// Returns the optional requested leader epoch.
    pub const fn requested_leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }

    /// Returns nullable Kafka metadata.
    pub fn requested_metadata(&self) -> Option<&str> {
        self.metadata.as_deref()
    }

    pub(crate) fn into_parts(self) -> (String, i32, i64, Option<i32>, Option<String>) {
        (
            self.topic,
            self.partition,
            self.next_offset,
            self.leader_epoch,
            self.metadata,
        )
    }
}
