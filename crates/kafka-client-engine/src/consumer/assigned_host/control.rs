//! Stable engine-owned scalar target for direct-consumer position control.

use std::sync::Arc;

use kafka_client_core::PartitionIndex;

const MAX_TOPIC_NAME_BYTES: usize = 249;

/// One topic-partition named at a direct-consumer control boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssignedConsumerPartition {
    pub(in crate::consumer) topic: Arc<str>,
    partition: i32,
}

impl AssignedConsumerPartition {
    /// Validates one stable topic-partition representation.
    pub fn try_new(
        topic: impl Into<Arc<str>>,
        partition: i32,
    ) -> Result<Self, AssignedConsumerPartitionInputError> {
        let topic = topic.into();
        if topic.is_empty() {
            return Err(AssignedConsumerPartitionInputError::new(
                AssignedConsumerPartitionInputErrorKind::EmptyTopic,
            ));
        }
        if topic.len() > MAX_TOPIC_NAME_BYTES {
            return Err(AssignedConsumerPartitionInputError::new(
                AssignedConsumerPartitionInputErrorKind::TopicTooLong,
            ));
        }
        if partition.is_negative() {
            return Err(AssignedConsumerPartitionInputError::new(
                AssignedConsumerPartitionInputErrorKind::NegativePartition,
            ));
        }
        Ok(Self { topic, partition })
    }

    /// Returns the retained topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the validated Kafka partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    pub(in crate::consumer) const fn partition_index(&self) -> PartitionIndex {
        PartitionIndex::from_raw(self.partition.cast_unsigned())
    }
}

/// Stable reason one control target is not representable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerPartitionInputErrorKind {
    /// Kafka topic names cannot be empty.
    EmptyTopic,
    /// The topic exceeds Kafka's 249-byte name limit.
    TopicTooLong,
    /// Kafka partitions are nonnegative signed 32-bit values.
    NegativePartition,
}

/// Rejection while constructing one direct-consumer control target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssignedConsumerPartitionInputError {
    kind: AssignedConsumerPartitionInputErrorKind,
}

impl AssignedConsumerPartitionInputError {
    const fn new(kind: AssignedConsumerPartitionInputErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable scalar-validation category.
    pub const fn kind(&self) -> AssignedConsumerPartitionInputErrorKind {
        self.kind
    }
}

impl std::fmt::Display for AssignedConsumerPartitionInputError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "invalid assigned-consumer partition: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AssignedConsumerPartitionInputError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedConsumerControlInputError {
    UnknownTopic,
    NegativeOffset,
}
