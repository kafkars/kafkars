//! Ordered terminal facts for one `IncrementalAlterConfigs` operation.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Exact broker-declared failure for one requested resource.
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

/// Per-resource incremental alteration result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncrementalAlterConfigResult {
    /// Kafka accepted every requested alteration for this topic.
    Altered,
    /// Kafka rejected this topic with an exact signed code.
    Failed(IncrementalAlterConfigBrokerError),
}

/// One resource result retained in original request order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigOutcome {
    resource_type: i8,
    resource_name: String,
    result: IncrementalAlterConfigResult,
}

impl IncrementalAlterConfigOutcome {
    /// Creates one successful topic result.
    pub fn altered(topic: impl Into<String>) -> Self {
        Self {
            resource_type: 2,
            resource_name: topic.into(),
            result: IncrementalAlterConfigResult::Altered,
        }
    }

    /// Creates one broker-rejected topic result.
    pub fn failed(topic: impl Into<String>, error: IncrementalAlterConfigBrokerError) -> Self {
        Self {
            resource_type: 2,
            resource_name: topic.into(),
            result: IncrementalAlterConfigResult::Failed(error),
        }
    }

    /// Creates one successful result for an exact requested resource.
    pub fn resource_altered(resource_type: i8, resource_name: impl Into<String>) -> Self {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            result: IncrementalAlterConfigResult::Altered,
        }
    }

    /// Creates one rejected result for an exact requested resource.
    pub fn resource_failed(
        resource_type: i8,
        resource_name: impl Into<String>,
        error: IncrementalAlterConfigBrokerError,
    ) -> Self {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            result: IncrementalAlterConfigResult::Failed(error),
        }
    }

    /// Returns Kafka's exact positive resource-type code.
    pub const fn resource_type(&self) -> i8 {
        self.resource_type
    }

    /// Returns the requested resource name.
    pub fn resource_name(&self) -> &str {
        &self.resource_name
    }

    /// Returns the requested topic.
    ///
    /// This compatibility accessor is intended for topic-scoped operations.
    pub fn topic(&self) -> &str {
        &self.resource_name
    }

    /// Returns the normalized topic result.
    pub const fn result(&self) -> &IncrementalAlterConfigResult {
        &self.result
    }

    /// Consumes this outcome into adapter-owned parts.
    pub fn into_parts(self) -> (String, IncrementalAlterConfigResult) {
        (self.resource_name, self.result)
    }

    /// Consumes this outcome into its exact resource identity and result.
    pub fn into_resource_parts(self) -> (i8, String, IncrementalAlterConfigResult) {
        (self.resource_type, self.resource_name, self.result)
    }
}

/// One successful batch plus Kafka's throttle observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigsBatch {
    throttle_time_ms: u32,
    resources: Vec<IncrementalAlterConfigOutcome>,
}

impl IncrementalAlterConfigsBatch {
    /// Creates one protocol-normalized ordered response batch.
    pub const fn new(throttle_time_ms: u32, resources: Vec<IncrementalAlterConfigOutcome>) -> Self {
        Self {
            throttle_time_ms,
            resources,
        }
    }

    /// Returns Kafka's nonnegative throttle observation without scheduling it.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns resource outcomes in original request order.
    pub fn resources(&self) -> &[IncrementalAlterConfigOutcome] {
        &self.resources
    }

    /// Returns topic-compatible outcomes in original request order.
    pub fn topics(&self) -> &[IncrementalAlterConfigOutcome] {
        &self.resources
    }

    /// Consumes the batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<IncrementalAlterConfigOutcome>) {
        (self.throttle_time_ms, self.resources)
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
