//! Bounded broker-declared failures for Admin `DeleteConsumerGroups`.

use core::num::NonZeroI16;

/// Maximum retained UTF-8 broker diagnostic prefix per consumer group.
pub const DELETE_CONSUMER_GROUPS_DIAGNOSTIC_BYTES: usize = 1024;

/// Exact broker-declared failure for one requested consumer group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteConsumerGroupsBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl DeleteConsumerGroupsBrokerError {
    /// Creates one exact signed Kafka group error without a broker diagnostic.
    pub const fn new(code: NonZeroI16) -> Self {
        Self {
            code,
            message: None,
            message_truncated: false,
        }
    }

    /// Creates one exact signed error with an already-bounded diagnostic.
    pub const fn with_bounded_message(
        code: NonZeroI16,
        message: Option<String>,
        message_truncated: bool,
    ) -> Self {
        Self {
            code,
            message,
            message_truncated,
        }
    }

    /// Returns Kafka's exact signed error code.
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
