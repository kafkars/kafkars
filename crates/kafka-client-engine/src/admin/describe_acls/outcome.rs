//! Stable engine terminal values for Admin `DescribeAcls`.

use core::fmt;

mod translate;

pub(crate) use translate::translate_terminal;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeAclsDeliveryStatus {
    /// The failed call did not reach Kafka.
    NotSent,
    /// The failed call may have reached Kafka.
    PossiblySent,
}

/// One described ACL binding with exact protocol-domain scalar values.
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
    /// Returns Kafka's exact concrete resource type.
    pub const fn resource_type(&self) -> i8 {
        self.resource_type
    }

    /// Returns the concrete resource name.
    pub fn resource_name(&self) -> &str {
        &self.resource_name
    }

    /// Returns Kafka's exact concrete resource-pattern type.
    pub const fn pattern_type(&self) -> i8 {
        self.pattern_type
    }

    /// Returns the principal identity.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Returns the host identity or wildcard.
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

    /// Consumes this binding into exact scalar parts.
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
}

/// Deterministically ordered bindings plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeAclsBatch {
    throttle_time_ms: u32,
    bindings: Vec<DescribeAclBinding>,
}

impl DescribeAclsBatch {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns bindings in core-determined scalar order.
    pub fn bindings(&self) -> &[DescribeAclBinding] {
        &self.bindings
    }

    /// Consumes throttle and deterministically ordered bindings.
    pub fn into_parts(self) -> (u32, Vec<DescribeAclBinding>) {
        (self.throttle_time_ms, self.bindings)
    }
}

/// Exact Kafka top-level rejection and bounded nullable diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeAclsBrokerError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl DescribeAclsBrokerError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code
    }

    /// Returns Kafka's nullable UTF-8-safe diagnostic prefix.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether a present diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes the rejection into exact diagnostic parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// Stable whole-operation failure category.
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
    /// A valid response exceeded the admitted retained envelope.
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
    delivery: DescribeAclsDeliveryStatus,
}

impl DescribeAclsFailure {
    /// Returns the stable failure category.
    pub const fn kind(&self) -> &DescribeAclsFailureKind {
        &self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(&self) -> DescribeAclsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeAclsOutcome {
    /// Kafka returned zero or more deterministically ordered bindings.
    Described(DescribeAclsBatch),
    /// The operation failed outside a valid binding set.
    Failed(DescribeAclsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeAclsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for DescribeAclsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin DescribeAcls result was already observed",
            Self::Stale => "Admin DescribeAcls observer is stale",
        })
    }
}

impl std::error::Error for DescribeAclsObserverError {}
