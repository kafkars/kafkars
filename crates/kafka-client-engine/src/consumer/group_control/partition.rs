//! Validated stable scalar target for classic-group batch control.

use std::sync::Arc;

const MAX_TOPIC_BYTES: usize = 249;

/// One caller-owned classic-group control target.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupConsumerPartition {
    topic: Arc<str>,
    partition: i32,
}

impl GroupConsumerPartition {
    /// Validates one topic spelling and zero-based partition.
    pub fn try_new(
        topic: impl Into<Arc<str>>,
        partition: i32,
    ) -> Result<Self, GroupConsumerPartitionInputError> {
        let topic = topic.into();
        let kind = if topic.is_empty() {
            Some(GroupConsumerPartitionInputErrorKind::EmptyTopic)
        } else if topic.len() > MAX_TOPIC_BYTES {
            Some(GroupConsumerPartitionInputErrorKind::TopicTooLong)
        } else if partition < 0 {
            Some(GroupConsumerPartitionInputErrorKind::NegativePartition)
        } else {
            None
        };
        match kind {
            Some(kind) => Err(GroupConsumerPartitionInputError { kind }),
            None => Ok(Self { topic, partition }),
        }
    }

    /// Returns the exact topic spelling.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the zero-based Kafka partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }
}

/// Stable scalar-input rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerPartitionInputErrorKind {
    /// The topic spelling is empty.
    EmptyTopic,
    /// The topic spelling exceeds Kafka's bounded UTF-8 byte domain.
    TopicTooLong,
    /// The partition is outside Kafka's nonnegative partition domain.
    NegativePartition,
}

/// Rejection of one invalid scalar control target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupConsumerPartitionInputError {
    kind: GroupConsumerPartitionInputErrorKind,
}

impl GroupConsumerPartitionInputError {
    /// Returns the stable invalid-input category.
    pub const fn kind(self) -> GroupConsumerPartitionInputErrorKind {
        self.kind
    }
}

impl core::fmt::Display for GroupConsumerPartitionInputError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            formatter,
            "invalid classic-group control partition: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupConsumerPartitionInputError {}
