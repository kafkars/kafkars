//! Deterministically ordered ACL bindings and terminal facts.

use core::cmp::Ordering;
use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Maximum retained UTF-8 broker diagnostic prefix.
pub const DESCRIBE_ACLS_DIAGNOSTIC_BYTES: usize = 1024;

/// One wire-free ACL binding using exact protocol-domain scalar values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeAclBinding {
    resource_type: i8,
    resource_name: String,
    pattern_type: i8,
    principal: String,
    host: String,
    operation: i8,
    permission_type: i8,
}

impl DescribeAclBinding {
    /// Creates one protocol-normalized binding for core validation.
    pub const fn new(
        resource_type: i8,
        resource_name: String,
        pattern_type: i8,
        principal: String,
        host: String,
        operation: i8,
        permission_type: i8,
    ) -> Self {
        Self {
            resource_type,
            resource_name,
            pattern_type,
            principal,
            host,
            operation,
            permission_type,
        }
    }

    /// Returns Kafka's exact concrete resource type.
    pub const fn resource_type(&self) -> i8 {
        self.resource_type
    }

    /// Returns the nonempty resource name.
    pub fn resource_name(&self) -> &str {
        &self.resource_name
    }

    /// Returns Kafka's exact concrete resource-pattern type.
    pub const fn pattern_type(&self) -> i8 {
        self.pattern_type
    }

    /// Returns the nonempty principal identity.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Returns the nonempty host identity or wildcard.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns Kafka's exact concrete ACL operation.
    pub const fn operation(&self) -> i8 {
        self.operation
    }

    /// Returns Kafka's exact concrete permission type.
    pub const fn permission_type(&self) -> i8 {
        self.permission_type
    }

    /// Consumes this binding into adapter-owned exact scalar parts.
    pub fn into_parts(self) -> (i8, String, i8, String, String, i8, i8) {
        (
            self.resource_type,
            self.resource_name,
            self.pattern_type,
            self.principal,
            self.host,
            self.operation,
            self.permission_type,
        )
    }

    pub(crate) fn deterministic_cmp(&self, other: &Self) -> Ordering {
        self.resource_name
            .as_bytes()
            .cmp(other.resource_name.as_bytes())
            .then_with(|| self.resource_type.cmp(&other.resource_type))
            .then_with(|| self.pattern_type.cmp(&other.pattern_type))
            .then_with(|| self.principal.as_bytes().cmp(other.principal.as_bytes()))
            .then_with(|| self.host.as_bytes().cmp(other.host.as_bytes()))
            .then_with(|| self.operation.cmp(&other.operation))
            .then_with(|| self.permission_type.cmp(&other.permission_type))
    }
}

/// Successful deterministic binding set plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeAclsBatch {
    throttle_time_ms: u32,
    bindings: Vec<DescribeAclBinding>,
}

impl DescribeAclsBatch {
    /// Creates one protocol-normalized batch for deterministic core validation.
    pub const fn new(throttle_time_ms: u32, bindings: Vec<DescribeAclBinding>) -> Self {
        Self {
            throttle_time_ms,
            bindings,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns bindings in deterministic scalar order.
    pub fn bindings(&self) -> &[DescribeAclBinding] {
        &self.bindings
    }

    /// Consumes the batch into throttle and ordered bindings.
    pub fn into_parts(self) -> (u32, Vec<DescribeAclBinding>) {
        (self.throttle_time_ms, self.bindings)
    }

    pub(crate) fn sort_bindings(&mut self) {
        self.bindings
            .sort_unstable_by(DescribeAclBinding::deterministic_cmp);
    }
}

/// Exact broker-declared top-level error and bounded nullable diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeAclsBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl DescribeAclsBrokerError {
    /// Creates one exact signed error with an already-bounded diagnostic.
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

    /// Returns Kafka's nullable UTF-8-safe diagnostic prefix.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether a present diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes the error into exact adapter-owned parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code.get(), self.message, self.message_truncated)
    }
}

/// Whole-operation failure category outside a valid ACL binding set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeAclsFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// Kafka rejected the query with an exact top-level error.
    Broker(DescribeAclsBrokerError),
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API version cannot represent required semantics.
    Compatibility,
    /// A response was malformed or could not be normalized.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeAclsFailure {
    kind: DescribeAclsFailureKind,
    delivery: DeliveryStatus,
}

impl DescribeAclsFailure {
    pub(crate) const fn new(kind: DescribeAclsFailureKind, delivery: DeliveryStatus) -> Self {
        Self { kind, delivery }
    }

    /// Returns the core-owned failure category.
    pub const fn kind(&self) -> &DescribeAclsFailureKind {
        &self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(&self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for Admin `DescribeAcls`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeAclsTerminal {
    /// Kafka returned zero or more deterministically ordered bindings.
    Described(DescribeAclsBatch),
    /// The whole operation failed outside a valid binding set.
    Failed(DescribeAclsFailure),
}
