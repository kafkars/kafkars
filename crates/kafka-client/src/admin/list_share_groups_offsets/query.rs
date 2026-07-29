//! Stable per-group selection for one multi-ShareGroup offset operation.

use crate::TopicPartition;

/// One ShareGroup identity and either all or selected topic-partitions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetsQuery {
    group_id: String,
    partitions: Option<Vec<TopicPartition>>,
}

impl ListShareGroupOffsetsQuery {
    /// Queries every broker-visible offset for one ShareGroup.
    pub fn all(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            partitions: None,
        }
    }

    /// Queries one caller-ordered nonempty partition selection for one ShareGroup.
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
