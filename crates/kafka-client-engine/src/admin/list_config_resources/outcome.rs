//! Stable generated-free terminals for Admin `ListConfigResources`.

use core::fmt;

use super::ListConfigResourcesListing;

mod translate;

pub(crate) use translate::translate_terminal;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListConfigResourcesDeliveryStatus {
    /// The failed call did not reach Kafka.
    NotSent,
    /// The failed call may have reached Kafka.
    PossiblySent,
}

/// Exact signed top-level Kafka rejection and its throttle observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListConfigResourcesBrokerError {
    pub(super) throttle_time_ms: u32,
    pub(super) code: i16,
}

impl ListConfigResourcesBrokerError {
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
pub enum ListConfigResourcesFailureKind {
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
pub struct ListConfigResourcesFailure {
    pub(super) kind: ListConfigResourcesFailureKind,
    pub(super) delivery: ListConfigResourcesDeliveryStatus,
}

impl ListConfigResourcesFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> ListConfigResourcesFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> ListConfigResourcesDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListConfigResourcesOutcome {
    /// Kafka returned zero or more canonical configuration resources.
    Listed(ListConfigResourcesListing),
    /// Kafka rejected the request with an exact top-level error.
    BrokerRejected(ListConfigResourcesBrokerError),
    /// Execution failed outside an exact Kafka rejection.
    Failed(ListConfigResourcesFailure),
}

/// Failure to observe one named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListConfigResourcesObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for ListConfigResourcesObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin ListConfigResources result was already observed",
            Self::Stale => "Admin ListConfigResources observer is stale",
        })
    }
}

impl std::error::Error for ListConfigResourcesObserverError {}
