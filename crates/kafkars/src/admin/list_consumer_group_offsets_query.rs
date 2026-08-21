//! Stable per-group partition selection for batched consumer-group offset listing.

use crate::TopicPartition;

/// One consumer-group identity and either all or selected topic-partitions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConsumerGroupOffsetsQuery {
    group_id: String,
    partitions: Option<Vec<TopicPartition>>,
}

impl ListConsumerGroupOffsetsQuery {
    /// Queries every broker-visible committed offset for one consumer group.
    pub fn all(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            partitions: None,
        }
    }

    /// Queries one caller-ordered nonempty partition selection for one group.
    ///
    /// Validation of emptiness, duplicate identities, partition values, and
    /// assignment-only start positions remains deferred until submission.
    pub fn selected<I>(group_id: impl Into<String>, partitions: I) -> Self
    where
        I: IntoIterator<Item = TopicPartition>,
    {
        Self {
            group_id: group_id.into(),
            partitions: Some(partitions.into_iter().collect()),
        }
    }

    pub(crate) fn into_parts(self) -> (String, Option<Vec<TopicPartition>>) {
        (self.group_id, self.partitions)
    }
}

impl From<String> for ListConsumerGroupOffsetsQuery {
    fn from(group_id: String) -> Self {
        Self::all(group_id)
    }
}

impl From<&str> for ListConsumerGroupOffsetsQuery {
    fn from(group_id: &str) -> Self {
        Self::all(group_id)
    }
}

impl From<&String> for ListConsumerGroupOffsetsQuery {
    fn from(group_id: &String) -> Self {
        Self::all(group_id)
    }
}
