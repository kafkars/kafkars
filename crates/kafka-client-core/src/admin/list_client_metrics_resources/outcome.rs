//! Bounded API-74 listing values, exact broker rejection, and terminal facts.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Maximum client-metrics resource names retained by one listing.
pub const LIST_CLIENT_METRICS_RESOURCES_MAX_RESOURCES: usize = 4 * 1024;
/// Maximum bytes retained for one client-metrics resource name.
pub const LIST_CLIENT_METRICS_RESOURCES_MAX_RESOURCE_NAME_BYTES: usize = i16::MAX as usize;
/// Conservative maximum retained result bytes owned through observation.
pub const LIST_CLIENT_METRICS_RESOURCES_MAX_RETAINED_BYTES: usize = 1024 * 1024;

/// Canonically ordered successful API-74 response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListClientMetricsResourcesListing {
    throttle_time_ms: u32,
    resource_names: Vec<String>,
}

impl ListClientMetricsResourcesListing {
    pub(crate) const fn new(throttle_time_ms: u32, resource_names: Vec<String>) -> Self {
        Self {
            throttle_time_ms,
            resource_names,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns unique resource names in strict UTF-8 byte order.
    pub fn resource_names(&self) -> &[String] {
        &self.resource_names
    }

    /// Consumes this listing into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<String>) {
        (self.throttle_time_ms, self.resource_names)
    }
}

/// Exact top-level API-74 broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListClientMetricsResourcesBrokerError {
    throttle_time_ms: u32,
    code: NonZeroI16,
}

impl ListClientMetricsResourcesBrokerError {
    /// Creates one exact broker rejection from a normalized response.
    pub const fn new(throttle_time_ms: u32, code: NonZeroI16) -> Self {
        Self {
            throttle_time_ms,
            code,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns Kafka's exact signed nonzero error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }

    /// Consumes this rejection into exact stable scalar parts.
    pub const fn into_parts(self) -> (u32, i16) {
        (self.throttle_time_ms, self.code.get())
    }
}

/// Whole-operation failure outside an exact API-74 broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListClientMetricsResourcesFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API version cannot represent version-zero semantics.
    Compatibility,
    /// A response was malformed or contradictory.
    InvalidResponse,
}

/// Whole-operation mechanism failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListClientMetricsResourcesFailure {
    kind: ListClientMetricsResourcesFailureKind,
    delivery: DeliveryStatus,
}

impl ListClientMetricsResourcesFailure {
    pub(crate) const fn new(
        kind: ListClientMetricsResourcesFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable mechanism-failure category.
    pub const fn kind(self) -> ListClientMetricsResourcesFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for one API-74 resource listing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListClientMetricsResourcesTerminal {
    /// Kafka returned zero or more canonical client-metrics resource names.
    Listed(ListClientMetricsResourcesListing),
    /// Kafka rejected the complete request with an exact top-level code.
    BrokerRejected(ListClientMetricsResourcesBrokerError),
    /// Execution failed outside an exact Kafka rejection.
    Failed(ListClientMetricsResourcesFailure),
}
