//! Ordered terminal facts for one topic `IncrementalAlterConfigs` operation.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Exact broker-declared failure for one requested topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl IncrementalAlterConfigBrokerError {
    /// Creates one exact signed broker error with a bounded diagnostic fact.
    pub const fn new(code: NonZeroI16, message: Option<String>, message_truncated: bool) -> Self {
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

    /// Returns the nullable bounded diagnostic.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Returns whether the engine shortened the diagnostic.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes the error into adapter-owned scalar parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code.get(), self.message, self.message_truncated)
    }
}

/// Per-topic incremental alteration result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncrementalAlterConfigResult {
    /// Kafka accepted every requested alteration for this topic.
    Altered,
    /// Kafka rejected this topic with an exact signed code.
    Failed(IncrementalAlterConfigBrokerError),
}

/// One topic result retained in original request order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigOutcome {
    topic: String,
    result: IncrementalAlterConfigResult,
}

impl IncrementalAlterConfigOutcome {
    /// Creates one successful topic result.
    pub fn altered(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            result: IncrementalAlterConfigResult::Altered,
        }
    }

    /// Creates one broker-rejected topic result.
    pub fn failed(topic: impl Into<String>, error: IncrementalAlterConfigBrokerError) -> Self {
        Self {
            topic: topic.into(),
            result: IncrementalAlterConfigResult::Failed(error),
        }
    }

    /// Returns the requested topic.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the normalized topic result.
    pub const fn result(&self) -> &IncrementalAlterConfigResult {
        &self.result
    }

    /// Consumes this outcome into adapter-owned parts.
    pub fn into_parts(self) -> (String, IncrementalAlterConfigResult) {
        (self.topic, self.result)
    }
}

/// One successful batch plus Kafka's throttle observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigsBatch {
    throttle_time_ms: u32,
    topics: Vec<IncrementalAlterConfigOutcome>,
}

impl IncrementalAlterConfigsBatch {
    /// Creates one protocol-normalized ordered response batch.
    pub const fn new(throttle_time_ms: u32, topics: Vec<IncrementalAlterConfigOutcome>) -> Self {
        Self {
            throttle_time_ms,
            topics,
        }
    }

    /// Returns Kafka's nonnegative throttle observation without scheduling it.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns topic outcomes in original request order.
    pub fn topics(&self) -> &[IncrementalAlterConfigOutcome] {
        &self.topics
    }

    /// Consumes the batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<IncrementalAlterConfigOutcome>) {
        (self.throttle_time_ms, self.topics)
    }
}

/// Whole-operation failure outside per-topic broker results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementalAlterConfigsFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected the request before transport ownership.
    DriverRejected,
    /// Transport failed after driver ownership.
    Transport,
    /// A broker response was malformed or could not be correlated.
    InvalidResponse,
    /// A structurally valid response exceeded retained terminal capacity.
    ResponseTooLarge,
    /// The broker cannot execute the requested incremental semantics.
    Compatibility,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IncrementalAlterConfigsFailure {
    kind: IncrementalAlterConfigsFailureKind,
    delivery: DeliveryStatus,
}

impl IncrementalAlterConfigsFailure {
    pub(crate) const fn new(
        kind: IncrementalAlterConfigsFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the deterministic failure category.
    pub const fn kind(self) -> IncrementalAlterConfigsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for an incremental configuration operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncrementalAlterConfigsTerminal {
    /// Ordered per-topic outcomes and retained throttle observation.
    Configs(IncrementalAlterConfigsBatch),
    /// Whole-operation failure outside per-topic results.
    Failed(IncrementalAlterConfigsFailure),
}
