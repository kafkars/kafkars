//! Ordered terminal facts for one bounded `DescribeConfigs` operation.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::DescribeConfigEntry;

/// Exact broker-declared failure for one requested resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl DescribeConfigBrokerError {
    /// Creates one exact signed broker error with a bounded diagnostic.
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

    /// Returns whether the diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }
}

/// Per-resource `DescribeConfigs` result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribeConfigResult {
    /// Kafka returned zero or more configuration entries.
    Configs(Vec<DescribeConfigEntry>),
    /// Kafka rejected this resource with an exact signed code.
    Failed(DescribeConfigBrokerError),
}

/// One per-resource result retained in original request order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigOutcome {
    resource_type: i8,
    resource_name: String,
    result: DescribeConfigResult,
}

impl DescribeConfigOutcome {
    /// Creates one successful resource outcome.
    pub fn described(
        resource_type: i8,
        resource_name: impl Into<String>,
        configs: Vec<DescribeConfigEntry>,
    ) -> Self {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            result: DescribeConfigResult::Configs(configs),
        }
    }

    /// Creates one resource-level broker rejection.
    pub fn failed(
        resource_type: i8,
        resource_name: impl Into<String>,
        error: DescribeConfigBrokerError,
    ) -> Self {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            result: DescribeConfigResult::Failed(error),
        }
    }

    /// Returns Kafka's resource type.
    pub const fn resource_type(&self) -> i8 {
        self.resource_type
    }

    /// Returns the requested resource name.
    pub fn resource_name(&self) -> &str {
        &self.resource_name
    }

    /// Returns this resource's normalized result.
    pub const fn result(&self) -> &DescribeConfigResult {
        &self.result
    }

    /// Consumes this ordered resource into adapter-owned parts.
    pub fn into_parts(self) -> (i8, String, DescribeConfigResult) {
        (self.resource_type, self.resource_name, self.result)
    }
}

/// One successful batch plus Kafka's retained throttle observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsBatch {
    throttle_time_ms: u32,
    resources: Vec<DescribeConfigOutcome>,
}

impl DescribeConfigsBatch {
    /// Creates one protocol-normalized response batch.
    pub const fn new(throttle_time_ms: u32, resources: Vec<DescribeConfigOutcome>) -> Self {
        Self {
            throttle_time_ms,
            resources,
        }
    }

    /// Returns Kafka's nonnegative throttle observation without scheduling policy.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns resources in original request order.
    pub fn resources(&self) -> &[DescribeConfigOutcome] {
        &self.resources
    }

    /// Consumes this batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<DescribeConfigOutcome>) {
        (self.throttle_time_ms, self.resources)
    }
}

/// Whole-operation failure outside per-resource broker results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeConfigsFailureKind {
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
    /// The selected protocol version cannot represent requested semantics.
    Compatibility,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DescribeConfigsFailure {
    kind: DescribeConfigsFailureKind,
    delivery: DeliveryStatus,
}

impl DescribeConfigsFailure {
    pub(crate) const fn new(kind: DescribeConfigsFailureKind, delivery: DeliveryStatus) -> Self {
        Self { kind, delivery }
    }

    /// Returns the deterministic failure category.
    pub const fn kind(self) -> DescribeConfigsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for a `DescribeConfigs` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribeConfigsTerminal {
    /// Ordered per-resource results and retained throttle observation.
    Configs(DescribeConfigsBatch),
    /// Whole-operation failure outside per-resource broker results.
    Failed(DescribeConfigsFailure),
}
