//! Stable ordered results for Admin `DeleteShareGroupOffsets`.

/// Exact topic-level Kafka rejection with its bounded diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteShareGroupOffsetsTopicError {
    pub(super) code: i16,
    pub(super) message: Option<String>,
    pub(super) message_truncated: bool,
}

impl DeleteShareGroupOffsetsTopicError {
    /// Returns Kafka's exact signed nonzero error code.
    pub const fn code(&self) -> i16 {
        self.code
    }

    /// Returns Kafka's optional bounded diagnostic.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether the diagnostic was truncated at the retained-byte bound.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes the rejection into exact stable parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// One caller-ordered topic deletion result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteShareGroupOffsetsTopicResult {
    pub(super) topic: String,
    pub(super) result: Result<[u8; 16], DeleteShareGroupOffsetsTopicError>,
}

impl DeleteShareGroupOffsetsTopicResult {
    /// Returns the exact requested topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the exact topic ID or topic-local Kafka rejection.
    pub const fn result(&self) -> &Result<[u8; 16], DeleteShareGroupOffsetsTopicError> {
        &self.result
    }

    /// Consumes this result into stable identity and exact broker outcome.
    pub fn into_parts(self) -> (String, Result<[u8; 16], DeleteShareGroupOffsetsTopicError>) {
        (self.topic, self.result)
    }
}

/// Ordered successful response plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteShareGroupOffsetsBatch {
    pub(super) throttle_time_ms: u32,
    pub(super) topics: Vec<DeleteShareGroupOffsetsTopicResult>,
}

impl DeleteShareGroupOffsetsBatch {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns results in original caller topic order.
    pub fn topics(&self) -> &[DeleteShareGroupOffsetsTopicResult] {
        &self.topics
    }

    /// Consumes the batch into throttle and caller-ordered results.
    pub fn into_parts(self) -> (u32, Vec<DeleteShareGroupOffsetsTopicResult>) {
        (self.throttle_time_ms, self.topics)
    }
}
