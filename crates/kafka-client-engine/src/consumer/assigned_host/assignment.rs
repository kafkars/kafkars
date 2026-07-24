//! Stable engine-owned scalar inputs for one direct-assignment replacement.

use std::sync::Arc;

use kafka_client_core::{PartitionIndex, StartPosition};

const MAX_TOPIC_NAME_BYTES: usize = 249;

/// Explicit initial-position policy for one directly assigned partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerStartPosition {
    /// Resolve Kafka's earliest available offset.
    Beginning,
    /// Resolve Kafka's end offset.
    End,
    /// Begin at this exact nonnegative next-fetch offset.
    Offset(i64),
}

/// One caller-ordered topic-partition and its required initial position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssignedConsumerAssignment {
    pub(in crate::consumer) topic: Arc<str>,
    partition: i32,
    pub(in crate::consumer) start: StartPosition,
}

impl AssignedConsumerAssignment {
    /// Validates one engine-owned assignment entry before operation admission.
    pub fn try_new(
        topic: impl Into<Arc<str>>,
        partition: i32,
        start: AssignedConsumerStartPosition,
    ) -> Result<Self, AssignedConsumerAssignmentInputError> {
        let topic = topic.into();
        if topic.is_empty() {
            return Err(AssignedConsumerAssignmentInputError::new(
                AssignedConsumerAssignmentInputErrorKind::EmptyTopic,
            ));
        }
        if topic.len() > MAX_TOPIC_NAME_BYTES {
            return Err(AssignedConsumerAssignmentInputError::new(
                AssignedConsumerAssignmentInputErrorKind::TopicTooLong,
            ));
        }
        if partition.is_negative() {
            return Err(AssignedConsumerAssignmentInputError::new(
                AssignedConsumerAssignmentInputErrorKind::NegativePartition,
            ));
        }
        let start = start
            .try_into_core()
            .ok_or(AssignedConsumerAssignmentInputError::new(
                AssignedConsumerAssignmentInputErrorKind::NegativeOffset,
            ))?;
        Ok(Self {
            topic,
            partition,
            start,
        })
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

    /// Returns the explicit initial-position policy.
    pub const fn start(&self) -> AssignedConsumerStartPosition {
        match self.start {
            StartPosition::Beginning => AssignedConsumerStartPosition::Beginning,
            StartPosition::End => AssignedConsumerStartPosition::End,
            StartPosition::Offset(offset) => AssignedConsumerStartPosition::Offset(offset.get()),
        }
    }

    #[cfg(test)]
    pub(crate) const fn new(
        topic: Arc<str>,
        partition: PartitionIndex,
        start: StartPosition,
    ) -> Self {
        Self {
            topic,
            partition: partition.get().cast_signed(),
            start,
        }
    }
}

impl AssignedConsumerStartPosition {
    pub(in crate::consumer) fn try_into_core(self) -> Option<StartPosition> {
        match self {
            Self::Beginning => Some(StartPosition::Beginning),
            Self::End => Some(StartPosition::End),
            Self::Offset(offset) => {
                kafka_client_core::NextFetchOffset::try_from_raw(offset).map(StartPosition::Offset)
            }
        }
    }
}

/// Stable reason one scalar assignment entry is not representable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerAssignmentInputErrorKind {
    /// Kafka topic names cannot be empty.
    EmptyTopic,
    /// The topic exceeds Kafka's 249-byte name limit.
    TopicTooLong,
    /// Kafka partitions are nonnegative signed 32-bit values.
    NegativePartition,
    /// Explicit next-fetch offsets are nonnegative.
    NegativeOffset,
}

/// Rejection while constructing one assignment entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssignedConsumerAssignmentInputError {
    kind: AssignedConsumerAssignmentInputErrorKind,
}

impl AssignedConsumerAssignmentInputError {
    const fn new(kind: AssignedConsumerAssignmentInputErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable scalar-validation category.
    pub const fn kind(&self) -> AssignedConsumerAssignmentInputErrorKind {
        self.kind
    }
}

impl std::fmt::Display for AssignedConsumerAssignmentInputError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "invalid assigned-consumer assignment entry: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AssignedConsumerAssignmentInputError {}

pub(crate) type AssignedPartitionInput = AssignedConsumerAssignment;
