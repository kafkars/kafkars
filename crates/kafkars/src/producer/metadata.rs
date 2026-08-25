//! Stable acknowledged-record metadata owned by the Rust facade.

use std::sync::Arc;

use crate::TopicUuid;

/// Metadata for one acknowledged record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMetadata {
    topic: Arc<str>,
    topic_uuid: Option<TopicUuid>,
    partition: i32,
    offset: i64,
    timestamp_milliseconds: Option<i64>,
    leader_epoch: Option<i32>,
    serialized_key_size: Option<usize>,
    serialized_value_size: Option<usize>,
}

impl RecordMetadata {
    #[cfg(test)]
    pub(crate) fn from_parts(
        topic: impl Into<Arc<str>>,
        partition: i32,
        offset: i64,
        timestamp_milliseconds: Option<i64>,
        leader_epoch: Option<i32>,
        serialized_key_size: Option<usize>,
        serialized_value_size: Option<usize>,
    ) -> Self {
        Self::from_parts_with_topic_uuid(
            topic,
            None,
            partition,
            offset,
            timestamp_milliseconds,
            leader_epoch,
            serialized_key_size,
            serialized_value_size,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the receipt retains each independent broker and serialization fact"
    )]
    pub(crate) fn from_parts_with_topic_uuid(
        topic: impl Into<Arc<str>>,
        topic_uuid: Option<TopicUuid>,
        partition: i32,
        offset: i64,
        timestamp_milliseconds: Option<i64>,
        leader_epoch: Option<i32>,
        serialized_key_size: Option<usize>,
        serialized_value_size: Option<usize>,
    ) -> Self {
        Self {
            topic: topic.into(),
            topic_uuid,
            partition,
            offset,
            timestamp_milliseconds,
            leader_epoch,
            serialized_key_size,
            serialized_value_size,
        }
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the topic UUID proven before this name-routed Produce attempt.
    ///
    /// This is client-observed pre-attempt evidence, not a UUID returned by the
    /// Produce response and not an atomic Kafka topic-identity binding.
    pub const fn topic_uuid(&self) -> Option<TopicUuid> {
        self.topic_uuid
    }

    /// Returns the acknowledged partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the acknowledged offset.
    pub const fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the broker timestamp when present.
    pub const fn timestamp_milliseconds(&self) -> Option<i64> {
        self.timestamp_milliseconds
    }

    /// Returns the acknowledged leader epoch when supplied by Kafka.
    pub const fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }

    /// Returns the exact serialized key length, or `None` when the key was null.
    ///
    /// `Some(0)` identifies a present empty key. This length excludes record,
    /// header, batch, compression, and request-envelope overhead.
    pub const fn serialized_key_size(&self) -> Option<usize> {
        self.serialized_key_size
    }

    /// Returns the exact serialized value length, or `None` when the value was null.
    ///
    /// `Some(0)` identifies a present empty value. This length excludes record,
    /// header, batch, compression, and request-envelope overhead.
    pub const fn serialized_value_size(&self) -> Option<usize> {
        self.serialized_value_size
    }
}
