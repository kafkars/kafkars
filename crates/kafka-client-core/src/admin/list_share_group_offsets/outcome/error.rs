//! Exact API-90 broker errors and mechanism failures.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Maximum UTF-8 bytes retained for one broker diagnostic prefix.
pub const LIST_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES: usize = 1024;

/// Exact broker-declared failure for one share-group topic-partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetsPartitionBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl ListShareGroupOffsetsPartitionBrokerError {
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

    /// Consumes this error into exact adapter-owned scalar parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code.get(), self.message, self.message_truncated)
    }
}

/// Exact group-level API-90 broker rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetsBrokerError {
    throttle_time_ms: u32,
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl ListShareGroupOffsetsBrokerError {
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

    /// Returns Kafka's exact signed nonzero group-level error code.
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

/// Whole-operation failure outside an exact API-90 broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListShareGroupOffsetsFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API version cannot represent required semantics.
    Compatibility,
    /// A broker response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation mechanism failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetsFailure {
    kind: ListShareGroupOffsetsFailureKind,
    delivery: DeliveryStatus,
}

impl ListShareGroupOffsetsFailure {
    pub(crate) const fn new(
        kind: ListShareGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable mechanism-failure category.
    pub const fn kind(self) -> ListShareGroupOffsetsFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}
