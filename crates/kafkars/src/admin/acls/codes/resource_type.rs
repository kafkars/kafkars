//! Exact signed Kafka ACL resource-type codes.

/// Kafka resource type used by ACL bindings and filters.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AclResourceType(i8);

impl AclResourceType {
    /// Unknown resource-type sentinel.
    pub const UNKNOWN: Self = Self(0);
    /// Filter wildcard for every resource type.
    pub const ANY: Self = Self(1);
    /// Kafka topic.
    pub const TOPIC: Self = Self(2);
    /// Consumer group.
    pub const GROUP: Self = Self(3);
    /// Kafka cluster.
    pub const CLUSTER: Self = Self(4);
    /// Transactional ID.
    pub const TRANSACTIONAL_ID: Self = Self(5);
    /// Delegation-token ID.
    pub const DELEGATION_TOKEN: Self = Self(6);
    /// User principal.
    pub const USER: Self = Self(7);

    /// Preserves one exact signed Kafka code, including future values.
    pub const fn from_code(code: i8) -> Self {
        Self(code)
    }

    /// Returns the exact signed Kafka code.
    pub const fn code(self) -> i8 {
        self.0
    }

    /// Reports whether this code can identify one concrete binding resource.
    pub const fn is_valid_for_binding(self) -> bool {
        self.code() > Self::ANY.code()
    }

    /// Reports whether this code can select resources in a filter.
    pub const fn is_valid_for_filter(self) -> bool {
        self.code() > Self::UNKNOWN.code()
    }
}
