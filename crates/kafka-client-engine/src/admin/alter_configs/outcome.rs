//! Engine-owned terminal representation for incremental configuration changes.

use core::fmt;

mod translate;

pub(crate) use translate::translate_terminal;

/// Stable delivery certainty independent of core types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncrementalAlterConfigsDeliveryStatus {
    /// The request definitely did not reach Kafka.
    NotSent,
    /// The request may have reached Kafka.
    PossiblySent,
}

/// Exact broker rejection for one requested resource.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalAlterConfigError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl IncrementalAlterConfigError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code
    }

    /// Returns the nullable bounded broker diagnostic.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Returns whether a diagnostic was shortened.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes the error into stable scalar parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// One resource result retained in original request order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalAlterConfigResult {
    resource_type: i8,
    resource_name: String,
    result: Result<(), IncrementalAlterConfigError>,
}

impl IncrementalAlterConfigResult {
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
    /// This compatibility accessor is intended for topic-scoped requests.
    pub fn topic(&self) -> &str {
        &self.resource_name
    }

    /// Returns this topic's broker outcome.
    pub const fn result(&self) -> &Result<(), IncrementalAlterConfigError> {
        &self.result
    }

    /// Consumes the result into adapter-owned parts.
    pub fn into_parts(self) -> (String, Result<(), IncrementalAlterConfigError>) {
        (self.resource_name, self.result)
    }

    /// Consumes the result into its exact resource identity and broker outcome.
    pub fn into_resource_parts(self) -> (i8, String, Result<(), IncrementalAlterConfigError>) {
        (self.resource_type, self.resource_name, self.result)
    }
}

/// Successful ordered response with Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalAlterConfigsResult {
    throttle_time_ms: u32,
    resources: Vec<IncrementalAlterConfigResult>,
}

impl IncrementalAlterConfigsResult {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns resource results in original request order.
    pub fn resources(&self) -> &[IncrementalAlterConfigResult] {
        &self.resources
    }

    /// Returns topic-compatible results in original request order.
    pub fn topics(&self) -> &[IncrementalAlterConfigResult] {
        &self.resources
    }

    /// Consumes the response into its scalar parts.
    pub fn into_parts(self) -> (u32, Vec<IncrementalAlterConfigResult>) {
        (self.throttle_time_ms, self.resources)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncrementalAlterConfigsFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected the request before ownership.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// The broker response could not be correlated.
    InvalidResponse,
    /// A valid response exceeded admitted result capacity.
    ResponseTooLarge,
    /// The negotiated API cannot represent incremental semantics.
    Compatibility,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IncrementalAlterConfigsFailure {
    kind: IncrementalAlterConfigsFailureKind,
    delivery: IncrementalAlterConfigsDeliveryStatus,
}

impl IncrementalAlterConfigsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> IncrementalAlterConfigsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> IncrementalAlterConfigsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncrementalAlterConfigsOutcome {
    /// Ordered per-topic outcomes and throttle observation.
    Configs(IncrementalAlterConfigsResult),
    /// Whole-operation failure.
    Failed(IncrementalAlterConfigsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncrementalAlterConfigsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for IncrementalAlterConfigsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "IncrementalAlterConfigs result was already observed",
            Self::Stale => "IncrementalAlterConfigs observer is stale",
        })
    }
}

impl std::error::Error for IncrementalAlterConfigsObserverError {}
