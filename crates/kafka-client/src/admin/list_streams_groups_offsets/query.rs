//! Stable per-group partition selection for batched Streams-group offset listing.

use crate::{TopicPartition, admin::ListConsumerGroupOffsetsQuery};

/// One Streams-group identity and either all or selected topic-partitions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListStreamsGroupOffsetsQuery {
    inner: ListConsumerGroupOffsetsQuery,
}

impl ListStreamsGroupOffsetsQuery {
    /// Queries every broker-visible committed offset for one Streams group.
    pub fn all(group_id: impl Into<String>) -> Self {
        Self {
            inner: ListConsumerGroupOffsetsQuery::all(group_id),
        }
    }

    /// Queries one caller-ordered nonempty partition selection for one group.
    ///
    /// Validation remains deferred until the plural builder is submitted.
    pub fn selected<I>(group_id: impl Into<String>, partitions: I) -> Self
    where
        I: IntoIterator<Item = TopicPartition>,
    {
        Self {
            inner: ListConsumerGroupOffsetsQuery::selected(group_id, partitions),
        }
    }

    pub(crate) fn into_consumer_group(self) -> ListConsumerGroupOffsetsQuery {
        self.inner
    }
}

impl From<String> for ListStreamsGroupOffsetsQuery {
    fn from(group_id: String) -> Self {
        Self::all(group_id)
    }
}

impl From<&str> for ListStreamsGroupOffsetsQuery {
    fn from(group_id: &str) -> Self {
        Self::all(group_id)
    }
}

impl From<&String> for ListStreamsGroupOffsetsQuery {
    fn from(group_id: &String) -> Self {
        Self::all(group_id)
    }
}
