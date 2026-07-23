//! Stable acknowledged-record metadata owned by the Rust facade.

/// Metadata for one acknowledged record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMetadata {
    topic: String,
    partition: i32,
    offset: i64,
    timestamp_milliseconds: Option<i64>,
    leader_epoch: Option<i32>,
}

impl RecordMetadata {
    pub(crate) const fn from_parts(
        topic: String,
        partition: i32,
        offset: i64,
        timestamp_milliseconds: Option<i64>,
        leader_epoch: Option<i32>,
    ) -> Self {
        Self {
            topic,
            partition,
            offset,
            timestamp_milliseconds,
            leader_epoch,
        }
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the acknowledged partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the acknowledged offset.
    pub const fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the broker timestamp when present.
    pub const fn timestamp_milliseconds(&self) -> Option<i64> {
        self.timestamp_milliseconds
    }

    /// Returns the acknowledged leader epoch when supplied by Kafka.
    pub const fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }
}
