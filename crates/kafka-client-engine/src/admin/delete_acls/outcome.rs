//! Stable positional engine terminal values for Admin `DeleteAcls`.

mod observer_error;
mod storage;
mod translate;

pub use observer_error::DeleteAclsObserverError;

#[cfg(test)]
pub(crate) use storage::DeleteAclsPrepareMatchingError;
pub(crate) use storage::DeleteAclsPreparedOutcomes;
pub(crate) use translate::{DeleteAclsTranslationError, translate_terminal_into};

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteAclsDeliveryStatus {
    /// The failed call did not reach Kafka.
    NotSent,
    /// The failed call may have reached Kafka.
    PossiblySent,
}

/// Exact broker-declared failure with a bounded nullable diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteAclBrokerError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl DeleteAclBrokerError {
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

    /// Consumes this rejection into exact diagnostic parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// Exact Kafka result for one concrete binding matching a deletion filter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteAclMatchResult {
    /// Kafka deleted this exact binding.
    Deleted,
    /// Kafka rejected deletion of this exact binding.
    BrokerFailed(DeleteAclBrokerError),
}

/// One exact concrete binding returned under a positional filter result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteAclMatchingBinding {
    resource_type: i8,
    resource_name: String,
    pattern_type: i8,
    principal: String,
    host: String,
    operation: i8,
    permission_type: i8,
    result: DeleteAclMatchResult,
}

impl DeleteAclMatchingBinding {
    /// Returns Kafka's exact concrete resource type.
    pub const fn resource_type(&self) -> i8 {
        self.resource_type
    }

    /// Returns the exact nonempty resource name.
    pub fn resource_name(&self) -> &str {
        &self.resource_name
    }

    /// Returns Kafka's exact concrete resource-pattern type.
    pub const fn pattern_type(&self) -> i8 {
        self.pattern_type
    }

    /// Returns the exact nonempty principal.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Returns the exact nonempty host or explicit wildcard.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns Kafka's exact concrete operation.
    pub const fn operation(&self) -> i8 {
        self.operation
    }

    /// Returns Kafka's exact concrete permission type.
    pub const fn permission_type(&self) -> i8 {
        self.permission_type
    }

    /// Returns Kafka's result for this exact matching binding.
    pub const fn result(&self) -> &DeleteAclMatchResult {
        &self.result
    }

    /// Consumes this binding into its exact owned parts.
    pub fn into_parts(self) -> (i8, String, i8, String, String, i8, i8, DeleteAclMatchResult) {
        (
            self.resource_type,
            self.resource_name,
            self.pattern_type,
            self.principal,
            self.host,
            self.operation,
            self.permission_type,
            self.result,
        )
    }
}

/// Exact result at one caller filter position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteAclFilterResult {
    /// Kafka evaluated the filter and returned zero or more exact bindings.
    Matched(Vec<DeleteAclMatchingBinding>),
    /// Kafka rejected this filter as a whole.
    BrokerFailed(DeleteAclBrokerError),
}

/// One caller-positioned filter and its exact corresponding result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteAclFilterOutcome {
    filter: super::DeleteAclsFilter,
    result: DeleteAclFilterResult,
}

impl DeleteAclFilterOutcome {
    /// Returns the original filter at this caller position.
    pub const fn filter(&self) -> &super::DeleteAclsFilter {
        &self.filter
    }

    /// Returns Kafka's exact corresponding filter result.
    pub const fn result(&self) -> &DeleteAclFilterResult {
        &self.result
    }

    /// Consumes this outcome into its filter and exact result.
    pub fn into_parts(self) -> (super::DeleteAclsFilter, DeleteAclFilterResult) {
        (self.filter, self.result)
    }
}

/// Caller-positioned settled filter results plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteAclsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<DeleteAclFilterOutcome>,
}

impl DeleteAclsBatch {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns outcomes in exact request order, including duplicate filters.
    pub fn outcomes(&self) -> &[DeleteAclFilterOutcome] {
        &self.outcomes
    }

    /// Consumes throttle and caller-positioned outcomes.
    pub fn into_parts(self) -> (u32, Vec<DeleteAclFilterOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteAclsFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded the admitted retained envelope.
    ResponseTooLarge,
    /// The selected API version cannot represent required semantics.
    Compatibility,
    /// A response was malformed or could not be normalized.
    InvalidResponse,
}

/// Whole-operation mechanism failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteAclsFailure {
    kind: DeleteAclsFailureKind,
    delivery: DeleteAclsDeliveryStatus,
}

impl DeleteAclsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> DeleteAclsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> DeleteAclsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteAclsOutcome {
    /// Every caller filter position has one exact result.
    Deleted(DeleteAclsBatch),
    /// The operation failed outside a complete positional result set.
    Failed(DeleteAclsFailure),
}
