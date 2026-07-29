//! Stable validation failures for consumer-group offset query intent.

use core::fmt;

/// Invalid deterministic group-offset query intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListConsumerGroupOffsetsPlanError {
    /// A batch operation must name at least one consumer group.
    EmptyGroupBatch,
    /// The query must name one explicit consumer group.
    EmptyGroupId,
    /// The UTF-8 group identity cannot fit the coordinator key domain.
    GroupIdTooLong,
    /// Selected mode must name at least one topic-partition.
    EmptySelection,
    /// A selected topic name must not be empty.
    EmptyTopicName,
    /// A selected topic name cannot fit Kafka's string domain.
    TopicNameTooLong,
    /// A selected partition index must be nonnegative.
    NegativePartition,
    /// Selected mode cannot repeat a topic-partition identity.
    DuplicateTopicPartition,
    /// One operation cannot retain more than 4,096 selected partitions.
    TooManySelectedPartitions,
    /// One accepted operation cannot retain more than the bounded group count.
    TooManyGroups,
    /// A batch operation cannot repeat a consumer-group identity.
    DuplicateGroupId,
    /// Aggregate request text exceeds the semantic bound.
    RequestTextTooLarge,
}

impl fmt::Display for ListConsumerGroupOffsetsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyGroupBatch => "consumer group batch is empty",
            Self::EmptyGroupId => "consumer group id is empty",
            Self::GroupIdTooLong => "consumer group id exceeds the coordinator key limit",
            Self::EmptySelection => "selected consumer group offset query is empty",
            Self::EmptyTopicName => "selected consumer group offset topic is empty",
            Self::TopicNameTooLong => {
                "selected consumer group offset topic exceeds Kafka's string limit"
            }
            Self::NegativePartition => "selected consumer group offset partition is negative",
            Self::DuplicateTopicPartition => {
                "selected consumer group offset query contains a duplicate topic-partition"
            }
            Self::TooManySelectedPartitions => {
                "consumer group offset query exceeds the selected partition limit"
            }
            Self::TooManyGroups => "consumer group batch exceeds the group-count limit",
            Self::DuplicateGroupId => "consumer group batch contains a duplicate group id",
            Self::RequestTextTooLarge => {
                "consumer group offset query exceeds the aggregate text byte limit"
            }
        })
    }
}

impl std::error::Error for ListConsumerGroupOffsetsPlanError {}
