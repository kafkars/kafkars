//! Stable resource-type values and validated API-74 v1 request intent.

use core::fmt;
use std::collections::BTreeSet;

/// Maximum explicit resource types retained by one request.
pub const LIST_CONFIG_RESOURCES_MAX_REQUEST_TYPES: usize = 32;

/// A positive Kafka configuration-resource type code.
///
/// Unknown positive values remain representable so adding a broker-side
/// resource type does not require a client API change.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct ConfigResourceType(i8);

impl ConfigResourceType {
    /// Topic configuration resources.
    pub const TOPIC: Self = Self(2);
    /// Broker configuration resources.
    pub const BROKER: Self = Self(4);
    /// Dynamically alterable broker logger resources.
    pub const BROKER_LOGGER: Self = Self(8);
    /// Client-metrics configuration resources.
    pub const CLIENT_METRICS: Self = Self(16);
    /// Consumer-group configuration resources.
    pub const GROUP: Self = Self(32);

    /// Creates one stable resource type while preserving future positive codes.
    pub const fn new(code: i8) -> Result<Self, ConfigResourceTypeError> {
        if code > 0 {
            Ok(Self(code))
        } else {
            Err(ConfigResourceTypeError::NonPositive)
        }
    }

    /// Returns Kafka's exact positive signed resource-type code.
    pub const fn code(self) -> i8 {
        self.0
    }
}

impl TryFrom<i8> for ConfigResourceType {
    type Error = ConfigResourceTypeError;

    fn try_from(code: i8) -> Result<Self, Self::Error> {
        Self::new(code)
    }
}

impl From<ConfigResourceType> for i8 {
    fn from(resource_type: ConfigResourceType) -> Self {
        resource_type.code()
    }
}

/// Invalid Kafka configuration-resource type code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigResourceTypeError {
    /// Request and successful response resource types must be positive.
    NonPositive,
}

impl fmt::Display for ConfigResourceTypeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Kafka configuration-resource type must be positive")
    }
}

impl std::error::Error for ConfigResourceTypeError {}

/// Validated caller-ordered API-74 v1 resource-type selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConfigResourcesPlan {
    resource_types: Vec<ConfigResourceType>,
}

impl ListConfigResourcesPlan {
    /// Validates a bounded unique selection; empty selects all resource types.
    pub fn new(
        resource_types: Vec<ConfigResourceType>,
    ) -> Result<Self, ListConfigResourcesPlanError> {
        if resource_types.len() > LIST_CONFIG_RESOURCES_MAX_REQUEST_TYPES {
            return Err(ListConfigResourcesPlanError::TooManyResourceTypes);
        }
        let mut unique = BTreeSet::new();
        if resource_types
            .iter()
            .any(|resource_type| !unique.insert(resource_type.code()))
        {
            return Err(ListConfigResourcesPlanError::DuplicateResourceType);
        }
        Ok(Self { resource_types })
    }

    /// Returns explicit resource types in exact caller order.
    ///
    /// An empty slice selects every resource type represented by the broker.
    pub fn resource_types(&self) -> &[ConfigResourceType] {
        &self.resource_types
    }

    /// Returns whether this plan requests all broker-represented resource types.
    pub fn lists_all_types(&self) -> bool {
        self.resource_types.is_empty()
    }
}

/// Invalid deterministic API-74 v1 request intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListConfigResourcesPlanError {
    /// One request cannot explicitly select more than 32 resource types.
    TooManyResourceTypes,
    /// One request cannot repeat the same exact resource-type code.
    DuplicateResourceType,
}

impl fmt::Display for ListConfigResourcesPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid ListConfigResources plan: {self:?}")
    }
}

impl std::error::Error for ListConfigResourcesPlanError {}
