//! Exact API-90 per-partition descriptions and results.

use super::ListShareGroupOffsetsPartitionBrokerError;

/// One successfully described share-partition position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetDescription {
    start_offset: Option<i64>,
    leader_epoch: Option<i32>,
    lag: Option<i64>,
}

impl ListShareGroupOffsetDescription {
    /// Creates one protocol-normalized share-partition description.
    pub const fn new(
        start_offset: Option<i64>,
        leader_epoch: Option<i32>,
        lag: Option<i64>,
    ) -> Self {
        Self {
            start_offset,
            leader_epoch,
            lag,
        }
    }

    /// Returns the share-partition start offset when Kafka supplied one.
    pub const fn start_offset(&self) -> Option<i64> {
        self.start_offset
    }

    /// Returns the partition leader epoch when Kafka supplied one.
    pub const fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }

    /// Returns the share-partition lag when the selected version supplied one.
    pub const fn lag(&self) -> Option<i64> {
        self.lag
    }

    /// Consumes this description into normalized scalar parts.
    pub const fn into_parts(self) -> (Option<i64>, Option<i32>, Option<i64>) {
        (self.start_offset, self.leader_epoch, self.lag)
    }
}

/// Exact result attached to one share-group topic-partition identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListShareGroupOffsetResult {
    /// Kafka returned normalized position facts.
    Described(ListShareGroupOffsetDescription),
    /// Kafka rejected this specific topic-partition.
    Failed(ListShareGroupOffsetsPartitionBrokerError),
}

/// One flat partition result normalized by the protocol seam.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetOutcome {
    topic: String,
    topic_id: [u8; 16],
    partition: i32,
    result: ListShareGroupOffsetResult,
}

impl ListShareGroupOffsetOutcome {
    /// Creates one successful partition result with its exact nonzero topic ID.
    pub const fn described(
        topic: String,
        topic_id: [u8; 16],
        partition: i32,
        description: ListShareGroupOffsetDescription,
    ) -> Self {
        Self {
            topic,
            topic_id,
            partition,
            result: ListShareGroupOffsetResult::Described(description),
        }
    }

    /// Creates one failed partition result with its exact broker fact.
    pub const fn failed(
        topic: String,
        topic_id: [u8; 16],
        partition: i32,
        error: ListShareGroupOffsetsPartitionBrokerError,
    ) -> Self {
        Self {
            topic,
            topic_id,
            partition,
            result: ListShareGroupOffsetResult::Failed(error),
        }
    }

    /// Returns the exact response topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns Kafka's exact nonzero topic identity.
    pub const fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    /// Returns the nonnegative partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the exact per-partition result.
    pub const fn result(&self) -> &ListShareGroupOffsetResult {
        &self.result
    }

    /// Consumes this outcome into adapter-owned parts.
    pub fn into_parts(self) -> (String, [u8; 16], i32, ListShareGroupOffsetResult) {
        (self.topic, self.topic_id, self.partition, self.result)
    }
}
