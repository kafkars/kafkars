//! Stable generated-free configuration resource identities and type codes.

/// Kafka configuration-resource type code.
///
/// Positive values not known by this client remain representable so a newer
/// broker does not lose resource identity through the facade.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConfigResourceType(i8);

impl ConfigResourceType {
    /// Topic configuration resources.
    #[allow(non_upper_case_globals)]
    pub const Topic: Self = Self(2);
    /// Broker configuration resources.
    #[allow(non_upper_case_globals)]
    pub const Broker: Self = Self(4);
    /// Broker-logger configuration resources.
    #[allow(non_upper_case_globals)]
    pub const BrokerLogger: Self = Self(8);
    /// Client-metrics configuration resources.
    #[allow(non_upper_case_globals)]
    pub const ClientMetrics: Self = Self(16);
    /// Consumer-group configuration resources.
    #[allow(non_upper_case_globals)]
    pub const Group: Self = Self(32);

    /// Retains a raw type code for validation at the submission boundary.
    pub const fn from_raw(value: i8) -> Self {
        Self(value)
    }

    /// Returns Kafka's exact signed type code.
    pub const fn as_raw(self) -> i8 {
        self.0
    }

    pub(crate) const fn from_engine(value: i8) -> Self {
        Self(value)
    }
}

/// One canonical configuration-resource identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigResource {
    resource_type: ConfigResourceType,
    name: String,
}

impl ConfigResource {
    pub(crate) const fn new(resource_type: ConfigResourceType, name: String) -> Self {
        Self {
            resource_type,
            name,
        }
    }

    /// Returns Kafka's exact resource type, including future positive values.
    pub const fn resource_type(&self) -> ConfigResourceType {
        self.resource_type
    }

    /// Returns the exact resource name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Consumes the identity into stable generated-free parts.
    pub fn into_parts(self) -> (ConfigResourceType, String) {
        (self.resource_type, self.name)
    }
}
