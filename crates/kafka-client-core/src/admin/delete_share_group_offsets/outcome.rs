//! Bounded API-92 topic results, exact broker rejections, and terminal facts.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Maximum UTF-8 bytes retained for one broker diagnostic prefix.
pub const DELETE_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES: usize = 1024;
/// Maximum aggregate topic-name and diagnostic bytes accepted from one response.
pub const DELETE_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TEXT_BYTES: usize = 1024 * 1024;
/// Maximum normalized terminal bytes retained by one operation.
pub const DELETE_SHARE_GROUP_OFFSETS_MAX_RETAINED_BYTES: usize = 2 * 1024 * 1024;

/// Exact broker-declared failure for one requested topic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteShareGroupOffsetsTopicBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl DeleteShareGroupOffsetsTopicBrokerError {
    /// Creates one exact signed error with an already-bounded diagnostic.
    pub const fn new(code: NonZeroI16, message: Option<String>, message_truncated: bool) -> Self {
        Self {
            code,
            message,
            message_truncated,
        }
    }

    /// Returns Kafka's exact signed nonzero topic error code.
    pub const fn code(&self) -> i16 {
        self.code.get()
    }

    /// Returns Kafka's nullable UTF-8-safe diagnostic prefix.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether a present diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes this error into exact adapter-owned scalar parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code.get(), self.message, self.message_truncated)
    }
}

/// Exact result attached to one requested share-group topic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteShareGroupOffsetsTopicResult {
    /// Kafka deleted the share-group offsets and identified the exact topic.
    Deleted([u8; 16]),
    /// Kafka rejected this specific topic.
    Failed(DeleteShareGroupOffsetsTopicBrokerError),
}

/// One per-topic result normalized by the protocol seam.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteShareGroupOffsetsTopicOutcome {
    topic: String,
    result: DeleteShareGroupOffsetsTopicResult,
}

impl DeleteShareGroupOffsetsTopicOutcome {
    /// Creates one successful topic result with Kafka's exact nonzero topic ID.
    pub const fn deleted(topic: String, topic_id: [u8; 16]) -> Self {
        Self {
            topic,
            result: DeleteShareGroupOffsetsTopicResult::Deleted(topic_id),
        }
    }

    /// Creates one failed topic result with its exact broker fact.
    pub const fn failed(topic: String, error: DeleteShareGroupOffsetsTopicBrokerError) -> Self {
        Self {
            topic,
            result: DeleteShareGroupOffsetsTopicResult::Failed(error),
        }
    }

    /// Returns the exact response topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the exact per-topic result.
    pub const fn result(&self) -> &DeleteShareGroupOffsetsTopicResult {
        &self.result
    }

    /// Consumes this outcome into adapter-owned parts.
    pub fn into_parts(self) -> (String, DeleteShareGroupOffsetsTopicResult) {
        (self.topic, self.result)
    }
}

/// Ordered successful API-92 response facts plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteShareGroupOffsetsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<DeleteShareGroupOffsetsTopicOutcome>,
}

impl DeleteShareGroupOffsetsBatch {
    /// Creates one protocol-normalized response batch for core correlation.
    pub const fn new(
        throttle_time_ms: u32,
        outcomes: Vec<DeleteShareGroupOffsetsTopicOutcome>,
    ) -> Self {
        Self {
            throttle_time_ms,
            outcomes,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns exactly one outcome per requested topic in caller order.
    pub fn outcomes(&self) -> &[DeleteShareGroupOffsetsTopicOutcome] {
        &self.outcomes
    }

    /// Consumes this batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<DeleteShareGroupOffsetsTopicOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Exact top-level API-92 broker rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteShareGroupOffsetsBrokerError {
    throttle_time_ms: u32,
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl DeleteShareGroupOffsetsBrokerError {
    /// Creates one exact signed rejection with an already-bounded diagnostic.
    pub const fn new(
        throttle_time_ms: u32,
        code: NonZeroI16,
        message: Option<String>,
        message_truncated: bool,
    ) -> Self {
        Self {
            throttle_time_ms,
            code,
            message,
            message_truncated,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns Kafka's exact signed nonzero top-level error code.
    pub const fn code(&self) -> i16 {
        self.code.get()
    }

    /// Returns Kafka's nullable UTF-8-safe diagnostic prefix.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether a present diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes this rejection into exact adapter-owned scalar parts.
    pub fn into_parts(self) -> (u32, i16, Option<String>, bool) {
        (
            self.throttle_time_ms,
            self.code.get(),
            self.message,
            self.message_truncated,
        )
    }
}

/// Whole-operation failure outside an exact API-92 broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteShareGroupOffsetsFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API version cannot represent API-92 v0 semantics.
    Compatibility,
    /// A broker response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation mechanism failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteShareGroupOffsetsFailure {
    kind: DeleteShareGroupOffsetsFailureKind,
    delivery: DeliveryStatus,
}

impl DeleteShareGroupOffsetsFailure {
    pub(crate) const fn new(
        kind: DeleteShareGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable mechanism-failure category.
    pub const fn kind(self) -> DeleteShareGroupOffsetsFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for one destructive API-92 operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteShareGroupOffsetsTerminal {
    /// Kafka returned one correlated outcome per requested topic.
    Deleted(DeleteShareGroupOffsetsBatch),
    /// Kafka rejected the complete request with an exact top-level error.
    BrokerRejected(DeleteShareGroupOffsetsBrokerError),
    /// Execution failed outside an exact Kafka rejection.
    Failed(DeleteShareGroupOffsetsFailure),
}
