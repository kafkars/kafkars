//! Bytes-native producer records and ordered duplicate-preserving headers.

use bytes::Bytes;

/// Crate-private ownership transfer for the engine bridge.
#[derive(Debug)]
pub(crate) struct RecordParts {
    pub(crate) topic: String,
    pub(crate) partition: Option<i32>,
    pub(crate) timestamp_milliseconds: Option<i64>,
    pub(crate) key: Option<Bytes>,
    pub(crate) value: Option<Bytes>,
    pub(crate) headers: Vec<Header>,
}

/// One Kafka record header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    name: String,
    value: Option<Bytes>,
}

impl Header {
    /// Creates a non-null header.
    pub fn new(name: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self {
            name: name.into(),
            value: Some(value.into()),
        }
    }

    /// Creates a header with a null value.
    pub fn null(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: None,
        }
    }

    /// Returns the header name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns nullable header bytes.
    pub fn value(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }

    pub(crate) const fn from_parts(name: String, value: Option<Bytes>) -> Self {
        Self { name, value }
    }

    pub(crate) fn into_parts(self) -> (String, Option<Bytes>) {
        (self.name, self.value)
    }
}

/// Owned, bytes-native record submitted to a producer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    topic: String,
    partition: Option<i32>,
    timestamp_milliseconds: Option<i64>,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<Header>,
}

impl Record {
    /// Begins a record for the named topic.
    pub fn to(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            partition: None,
            timestamp_milliseconds: None,
            key: None,
            value: None,
            headers: Vec::new(),
        }
    }

    /// Creates an explicit tombstone record.
    pub fn tombstone(topic: impl Into<String>) -> Self {
        Self::to(topic)
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

    /// Appends a header without deduplicating its name.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.push(Header::new(name, value));
        self
    }

    /// Returns the logical topic name.
    pub fn topic(&self) -> &str {
        &self.topic
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

    pub(crate) fn from_parts(parts: RecordParts) -> Self {
        Self {
            topic: parts.topic,
            partition: parts.partition,
            timestamp_milliseconds: parts.timestamp_milliseconds,
            key: parts.key,
            value: parts.value,
            headers: parts.headers,
        }
    }

    pub(crate) fn into_parts(self) -> RecordParts {
        RecordParts {
            topic: self.topic,
            partition: self.partition,
            timestamp_milliseconds: self.timestamp_milliseconds,
            key: self.key,
            value: self.value,
            headers: self.headers,
        }
    }
}
