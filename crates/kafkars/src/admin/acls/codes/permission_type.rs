//! Exact signed Kafka ACL permission-type codes.

/// Kafka ACL permission decision or filter selector.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AclPermissionType(i8);

impl AclPermissionType {
    /// Unknown permission-type sentinel.
    pub const UNKNOWN: Self = Self(0);
    /// Filter wildcard for every permission type.
    pub const ANY: Self = Self(1);
    /// Deny matching access.
    pub const DENY: Self = Self(2);
    /// Allow matching access.
    pub const ALLOW: Self = Self(3);

    /// Preserves one exact signed Kafka code, including future values.
    pub const fn from_code(code: i8) -> Self {
        Self(code)
    }

    /// Returns the exact signed Kafka code.
    pub const fn code(self) -> i8 {
        self.0
    }

    /// Reports whether this code can be stored in one concrete ACL entry.
    pub const fn is_valid_for_binding(self) -> bool {
        self.code() > Self::ANY.code()
    }

    /// Reports whether this code can select permission decisions in a filter.
    pub const fn is_valid_for_filter(self) -> bool {
        self.code() > Self::UNKNOWN.code()
    }
}
