//! Exact validation failures for singular and batched API-90 request intent.

use core::fmt;

/// Invalid deterministic share-group offset listing intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListShareGroupOffsetsPlanError {
    /// A batch operation must name at least one share group.
    EmptyGroupBatch,
    /// The request must name one explicit share group.
    EmptyGroupId,
    /// The group identity cannot fit Kafka's string domain.
    GroupIdTooLong,
    /// Selected mode must contain at least one topic-partition.
    EmptySelection,
    /// One accepted operation cannot retain more than 4096 selected partitions.
    TooManySelectedPartitions,
    /// One operation cannot retain more than the bounded share-group count.
    TooManyGroups,
    /// One operation cannot repeat a share-group identity.
    DuplicateGroupId,
    /// Selected topic names must not be empty.
    EmptyTopicName,
    /// A selected topic name cannot fit Kafka's string domain.
    TopicNameTooLong,
    /// Partition indices must be nonnegative.
    NegativePartition,
    /// One selected request cannot repeat a topic-partition identity.
    DuplicateTopicPartition,
    /// Aggregate request text exceeds the one-MiB semantic bound.
    RequestTextTooLarge,
}

impl fmt::Display for ListShareGroupOffsetsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid ListShareGroupOffsets plan: {self:?}")
    }
}

impl std::error::Error for ListShareGroupOffsetsPlanError {}
