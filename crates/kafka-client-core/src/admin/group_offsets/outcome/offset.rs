//! Exact normalized topic-partition offset outcomes.

use core::num::NonZeroI16;

/// Exact broker-declared failure for one topic-partition offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupOffsetBrokerError {
    code: NonZeroI16,
}

impl GroupOffsetBrokerError {
    /// Creates one exact signed Kafka partition error.
    pub const fn new(code: NonZeroI16) -> Self {
        Self { code }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }
}

/// One successfully committed next-offset description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupOffsetDescription {
    offset: Option<i64>,
    leader_epoch: Option<i32>,
    metadata: Option<String>,
}

impl GroupOffsetDescription {
    /// Creates one normalized committed-offset description.
    pub const fn new(
        offset: Option<i64>,
        leader_epoch: Option<i32>,
        metadata: Option<String>,
    ) -> Self {
        Self {
            offset,
            leader_epoch,
            metadata,
        }
    }

    /// Returns the committed next offset, or absence when no offset exists.
    pub const fn offset(&self) -> Option<i64> {
        self.offset
    }

    /// Returns the committed leader epoch when Kafka supplied one.
    pub const fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }

    /// Returns nullable committed metadata without collapsing an empty value.
    pub fn metadata(&self) -> Option<&str> {
        self.metadata.as_deref()
    }

    /// Consumes the description into adapter-owned scalar values.
    pub fn into_parts(self) -> (Option<i64>, Option<i32>, Option<String>) {
        (self.offset, self.leader_epoch, self.metadata)
    }
}

/// Exact result attached to one ordered topic-partition identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupOffsetResult {
    /// Kafka returned a committed offset description.
    Described(GroupOffsetDescription),
    /// Kafka rejected this specific topic-partition.
    Failed(GroupOffsetBrokerError),
}

/// One normalized topic-partition result in deterministic order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupOffsetOutcome {
    topic: String,
    partition: i32,
    result: GroupOffsetResult,
}

impl GroupOffsetOutcome {
    /// Creates one successful topic-partition result.
    pub const fn described(
        topic: String,
        partition: i32,
        description: GroupOffsetDescription,
    ) -> Self {
        Self {
            topic,
            partition,
            result: GroupOffsetResult::Described(description),
        }
    }

    /// Creates one failed topic-partition result.
    pub const fn failed(topic: String, partition: i32, error: GroupOffsetBrokerError) -> Self {
        Self {
            topic,
            partition,
            result: GroupOffsetResult::Failed(error),
        }
    }

    /// Returns the normalized topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the normalized partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the exact normalized partition result.
    pub const fn result(&self) -> &GroupOffsetResult {
        &self.result
    }

    /// Consumes this outcome into adapter-owned parts.
    pub fn into_parts(self) -> (String, i32, GroupOffsetResult) {
        (self.topic, self.partition, self.result)
    }
}
