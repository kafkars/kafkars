//! Stable generated-free terminals for Admin `ListClientMetricsResources`.

use core::fmt;

mod translate;

pub(crate) use translate::translate_terminal;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListClientMetricsResourcesDeliveryStatus {
    /// The failed call did not reach Kafka.
    NotSent,
    /// The failed call may have reached Kafka.
    PossiblySent,
}

/// Bounded client-metrics resource names and Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListClientMetricsResourcesListing {
    pub(super) throttle_time_ms: u32,
    pub(super) resource_names: Vec<String>,
}

impl ListClientMetricsResourcesListing {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns unique resource names in strict UTF-8 byte order.
    pub fn resource_names(&self) -> &[String] {
        &self.resource_names
    }

    /// Consumes the listing into stable scalar parts.
    pub fn into_parts(self) -> (u32, Vec<String>) {
        (self.throttle_time_ms, self.resource_names)
    }
}

/// Exact signed top-level Kafka rejection and its throttle observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListClientMetricsResourcesBrokerError {
    pub(super) throttle_time_ms: u32,
    pub(super) code: i16,
}

impl ListClientMetricsResourcesBrokerError {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns Kafka's exact signed nonzero error code.
    pub const fn code(self) -> i16 {
        self.code
    }

    /// Consumes the rejection into exact scalar parts.
    pub const fn into_parts(self) -> (u32, i16) {
        (self.throttle_time_ms, self.code)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListClientMetricsResourcesFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// Valid response facts exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API version cannot represent required semantics.
    Compatibility,
    /// A response was malformed or could not be normalized.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListClientMetricsResourcesFailure {
    pub(super) kind: ListClientMetricsResourcesFailureKind,
    pub(super) delivery: ListClientMetricsResourcesDeliveryStatus,
}

impl ListClientMetricsResourcesFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> ListClientMetricsResourcesFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> ListClientMetricsResourcesDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListClientMetricsResourcesOutcome {
    /// Kafka returned zero or more canonical resource names.
    Listed(ListClientMetricsResourcesListing),
    /// Kafka rejected the request with an exact top-level error.
    BrokerRejected(ListClientMetricsResourcesBrokerError),
    /// Execution failed outside an exact Kafka rejection.
    Failed(ListClientMetricsResourcesFailure),
}

/// Failure to observe one named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListClientMetricsResourcesObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for ListClientMetricsResourcesObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin ListClientMetricsResources result was already observed",
            Self::Stale => "Admin ListClientMetricsResources observer is stale",
        })
    }
}

impl std::error::Error for ListClientMetricsResourcesObserverError {}
