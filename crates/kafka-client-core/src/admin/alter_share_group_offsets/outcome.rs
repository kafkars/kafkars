//! Bounded API-91 partition results, group rejections, and terminal facts.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Maximum UTF-8 bytes retained for one broker diagnostic prefix.
pub const ALTER_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES: usize = 1024;
/// Maximum response topic-partitions accepted by one operation.
pub const ALTER_SHARE_GROUP_OFFSETS_MAX_RESPONSE_PARTITIONS: usize = 4 * 1024;
/// Maximum aggregate topic-name and diagnostic bytes accepted from one response.
pub const ALTER_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TEXT_BYTES: usize = 1024 * 1024;
/// Maximum normalized terminal bytes retained by one operation.
pub const ALTER_SHARE_GROUP_OFFSETS_MAX_RETAINED_BYTES: usize = 2 * 1024 * 1024;

/// Exact broker-declared failure for one requested topic-partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterShareGroupOffsetsPartitionBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl AlterShareGroupOffsetsPartitionBrokerError {
    /// Creates one exact signed error with an already-bounded diagnostic.
    pub const fn new(code: NonZeroI16, message: Option<String>, message_truncated: bool) -> Self {
        Self {
            code,
            message,
            message_truncated,
        }
    }

    /// Returns Kafka's exact signed nonzero partition error code.
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

    /// Consumes this error into adapter-owned scalar parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code.get(), self.message, self.message_truncated)
    }
}

/// Exact result attached to one requested share-group topic-partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterShareGroupOffsetsPartitionResult {
    /// Kafka accepted the requested starting offset.
    Altered,
    /// Kafka rejected this specific topic-partition.
    Failed(AlterShareGroupOffsetsPartitionBrokerError),
}

/// One per-partition result normalized by the protocol seam.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterShareGroupOffsetsPartitionOutcome {
    topic: String,
    topic_id: [u8; 16],
    partition: i32,
    result: AlterShareGroupOffsetsPartitionResult,
}

impl AlterShareGroupOffsetsPartitionOutcome {
    /// Creates one successful result with Kafka's exact nonzero topic ID.
    pub const fn altered(topic: String, topic_id: [u8; 16], partition: i32) -> Self {
        Self {
            topic,
            topic_id,
            partition,
            result: AlterShareGroupOffsetsPartitionResult::Altered,
        }
    }

    /// Creates one failed result with Kafka's topic ID and exact broker fact.
    pub const fn failed(
        topic: String,
        topic_id: [u8; 16],
        partition: i32,
        error: AlterShareGroupOffsetsPartitionBrokerError,
    ) -> Self {
        Self {
            topic,
            topic_id,
            partition,
            result: AlterShareGroupOffsetsPartitionResult::Failed(error),
        }
    }

    /// Returns the exact response topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns Kafka's exact nonzero topic identity.
    pub const fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    /// Returns the exact nonnegative partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the exact per-partition result.
    pub const fn result(&self) -> &AlterShareGroupOffsetsPartitionResult {
        &self.result
    }

    /// Consumes this outcome into adapter-owned parts.
    pub fn into_parts(self) -> (String, [u8; 16], i32, AlterShareGroupOffsetsPartitionResult) {
        (self.topic, self.topic_id, self.partition, self.result)
    }
}

/// Ordered successful API-91 response facts plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterShareGroupOffsetsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<AlterShareGroupOffsetsPartitionOutcome>,
}

impl AlterShareGroupOffsetsBatch {
    /// Creates one protocol-normalized response batch for core correlation.
    pub const fn new(
        throttle_time_ms: u32,
        outcomes: Vec<AlterShareGroupOffsetsPartitionOutcome>,
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

    /// Returns exactly one outcome per requested partition in caller order.
    pub fn outcomes(&self) -> &[AlterShareGroupOffsetsPartitionOutcome] {
        &self.outcomes
    }

    /// Consumes this batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<AlterShareGroupOffsetsPartitionOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Exact top-level API-91 share-group rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterShareGroupOffsetsBrokerError {
    throttle_time_ms: u32,
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl AlterShareGroupOffsetsBrokerError {
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

/// Whole-operation failure outside an exact API-91 broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterShareGroupOffsetsFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API version cannot represent API-91 v0 semantics.
    Compatibility,
    /// A broker response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation mechanism failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlterShareGroupOffsetsFailure {
    kind: AlterShareGroupOffsetsFailureKind,
    delivery: DeliveryStatus,
}

impl AlterShareGroupOffsetsFailure {
    pub(crate) const fn new(
        kind: AlterShareGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable mechanism-failure category.
    pub const fn kind(self) -> AlterShareGroupOffsetsFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for one destructive API-91 operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterShareGroupOffsetsTerminal {
    /// Kafka returned one correlated outcome per requested topic-partition.
    Altered(AlterShareGroupOffsetsBatch),
    /// Kafka rejected the complete group request with an exact top-level error.
    BrokerRejected(AlterShareGroupOffsetsBrokerError),
    /// Execution failed outside an exact Kafka rejection.
    Failed(AlterShareGroupOffsetsFailure),
}
