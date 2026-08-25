//! Bytes-native producer records and ordered duplicate-preserving headers.

mod transfer;

use std::sync::Arc;

use bytes::Bytes;

use crate::{
    TopicUuid,
    header_name::{HeaderName, SourceOwner},
};

#[cfg(test)]
pub(crate) use transfer::RecordParts;
pub(crate) use transfer::RecordTransferParts;

/// One Kafka record header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    name: HeaderName,
    value: Option<Bytes>,
}

impl Header {
    /// Creates a non-null header.
    pub fn new(name: impl Into<HeaderName>, value: impl Into<Bytes>) -> Self {
        Self {
            name: name.into(),
            value: Some(value.into()),
        }
    }

    /// Creates a header with a null value.
    pub fn null(name: impl Into<HeaderName>) -> Self {
        Self {
            name: name.into(),
            value: None,
        }
    }

    /// Returns the header name.
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Returns the cloneable shared validated name owner.
    pub const fn header_name(&self) -> &HeaderName {
        &self.name
    }

    /// Returns nullable header bytes.
    pub fn value(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn from_parts(name: impl Into<HeaderName>, value: Option<Bytes>) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }

    pub(crate) fn from_shared_parts(
        name: Bytes,
        value: Option<Bytes>,
        source_owner: SourceOwner,
    ) -> Self {
        Self {
            name: HeaderName::from_shared(name, source_owner),
            value,
        }
    }

    pub(crate) fn into_parts(self) -> (HeaderName, Option<Bytes>) {
        (self.name, self.value)
    }
}

/// Owned, bytes-native record submitted to a producer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    topic: Arc<str>,
    expected_topic_uuid: Option<TopicUuid>,
    partition: Option<i32>,
    timestamp_milliseconds: Option<i64>,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<Header>,
    source_owner: SourceOwner,
}

impl Record {
    /// Begins a record for the named topic.
    ///
    /// An `Arc<str>` crosses this boundary without reallocating its topic bytes,
    /// allowing repeated records to share one canonical topic owner.
    pub fn to(topic: impl Into<Arc<str>>) -> Self {
        Self {
            topic: topic.into(),
            expected_topic_uuid: None,
            partition: None,
            timestamp_milliseconds: None,
            key: None,
            value: None,
            headers: Vec::new(),
            source_owner: SourceOwner::none(),
        }
    }

    /// Creates an explicit tombstone record.
    pub fn tombstone(topic: impl Into<Arc<str>>) -> Self {
        Self::to(topic)
    }

    /// Requires the named topic to retain this exact broker-issued UUID.
    ///
    /// Kafkars validates a broker topic view before a name-routed Produce
    /// attempt and before each replacement attempt. Kafka Produce does not
    /// atomically carry or return this UUID, so the deployment must prevent
    /// deletion and recreation from the final proof through the terminal
    /// result. Transactions require the stronger fence documented by
    /// [`crate::Transaction::validate_for_commit`].
    #[must_use]
    pub const fn expected_topic_uuid(mut self, topic_uuid: TopicUuid) -> Self {
        self.expected_topic_uuid = Some(topic_uuid);
        self
    }

    /// Selects an explicit partition instead of automatic partitioning.
    pub const fn partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Sets the optional record timestamp.
    pub const fn timestamp_milliseconds(mut self, timestamp: i64) -> Self {
        self.timestamp_milliseconds = Some(timestamp);
        self
    }

    /// Sets a non-null key.
    pub fn key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Sets a non-null value. Empty bytes remain distinct from a tombstone.
    pub fn value(mut self, value: impl Into<Bytes>) -> Self {
        self.value = Some(value.into());
        self
    }

    /// Appends one prebuilt nullable or non-null header without deduplicating its name.
    pub fn with_header(mut self, header: Header) -> Self {
        self.headers.push(header);
        self
    }

    /// Appends one non-null header without deduplicating its name.
    pub fn header(self, name: impl Into<HeaderName>, value: impl Into<Bytes>) -> Self {
        self.with_header(Header::new(name, value))
    }

    /// Returns the logical topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub(crate) const fn topic_owner(&self) -> &Arc<str> {
        &self.topic
    }

    /// Returns the exact broker-issued topic UUID required before admission.
    pub const fn expected_topic_uuid_value(&self) -> Option<TopicUuid> {
        self.expected_topic_uuid
    }

    /// Returns the explicit partition, when configured.
    pub const fn explicit_partition(&self) -> Option<i32> {
        self.partition
    }

    /// Returns the optional timestamp.
    pub const fn timestamp(&self) -> Option<i64> {
        self.timestamp_milliseconds
    }

    /// Returns the nullable key.
    pub fn key_bytes(&self) -> Option<&Bytes> {
        self.key.as_ref()
    }

    /// Returns the nullable value.
    pub fn value_bytes(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }

    /// Returns ordered headers, including duplicate names.
    pub fn headers(&self) -> &[Header] {
        &self.headers
    }
}
