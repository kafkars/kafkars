//! Bounded API-74 v1 listing values, exact rejection, and terminal facts.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::ConfigResourceType;

/// Maximum resources retained from one complete response.
pub const LIST_CONFIG_RESOURCES_MAX_RESOURCES: usize = 4 * 1024;
/// Maximum UTF-8 bytes retained for one resource name.
pub const LIST_CONFIG_RESOURCES_MAX_RESOURCE_NAME_BYTES: usize = 256;
/// Maximum aggregate resource-name bytes retained from one complete response.
pub const LIST_CONFIG_RESOURCES_MAX_TEXT_BYTES: usize = 1024 * 1024;

/// One protocol-normalized successful configuration resource.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListedConfigResource {
    resource_type: ConfigResourceType,
    resource_name: String,
}

impl ListedConfigResource {
    /// Creates one resource for complete-response validation by core.
    pub const fn new(resource_type: ConfigResourceType, resource_name: String) -> Self {
        Self {
            resource_type,
            resource_name,
        }
    }

    /// Returns Kafka's stable positive configuration-resource type.
    pub const fn resource_type(&self) -> ConfigResourceType {
        self.resource_type
    }

    /// Returns the nonempty resource name.
    pub fn resource_name(&self) -> &str {
        &self.resource_name
    }

    /// Consumes this resource into adapter-owned stable parts.
    pub fn into_parts(self) -> (ConfigResourceType, String) {
        (self.resource_type, self.resource_name)
    }
}

/// Canonically ordered successful API-74 v1 response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConfigResourcesListing {
    throttle_time_ms: u32,
    resources: Vec<ListedConfigResource>,
}

impl ListConfigResourcesListing {
    pub(crate) const fn new(throttle_time_ms: u32, resources: Vec<ListedConfigResource>) -> Self {
        Self {
            throttle_time_ms,
            resources,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns unique resources ordered by signed type then name bytes.
    pub fn resources(&self) -> &[ListedConfigResource] {
        &self.resources
    }

    /// Consumes this listing into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<ListedConfigResource>) {
        (self.throttle_time_ms, self.resources)
    }
}

/// Exact top-level API-74 v1 broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListConfigResourcesBrokerError {
    throttle_time_ms: u32,
    code: NonZeroI16,
}

impl ListConfigResourcesBrokerError {
    /// Creates one exact rejection from a protocol-normalized response.
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

    /// Returns Kafka's exact signed nonzero top-level error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }

    /// Consumes this rejection into exact stable scalar parts.
    pub const fn into_parts(self) -> (u32, i16) {
        (self.throttle_time_ms, self.code.get())
    }
}

/// Whole-operation failure outside an exact broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListConfigResourcesFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API version cannot represent the requested semantics.
    Compatibility,
    /// A response was malformed or contradictory.
    InvalidResponse,
}

/// Whole-operation mechanism failure with delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListConfigResourcesFailure {
    kind: ListConfigResourcesFailureKind,
    delivery: DeliveryStatus,
}

impl ListConfigResourcesFailure {
    pub(crate) const fn new(
        kind: ListConfigResourcesFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable mechanism-failure category.
    pub const fn kind(self) -> ListConfigResourcesFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for one API-74 v1 resource listing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListConfigResourcesTerminal {
    /// Kafka returned zero or more canonical configuration resources.
    Listed(ListConfigResourcesListing),
    /// Kafka rejected the complete request with an exact top-level code.
    BrokerRejected(ListConfigResourcesBrokerError),
    /// Execution failed outside an exact Kafka rejection.
    Failed(ListConfigResourcesFailure),
}
