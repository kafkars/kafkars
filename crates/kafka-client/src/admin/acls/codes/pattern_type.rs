//! Exact signed Kafka ACL resource-pattern codes.

/// Kafka resource-pattern type used by ACL bindings and filters.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AclPatternType(i8);

impl AclPatternType {
    /// Unknown pattern-type sentinel.
    pub const UNKNOWN: Self = Self(0);
    /// Filter wildcard for every pattern type.
    pub const ANY: Self = Self(1);
    /// Filter selector matching literal, wildcard, or prefixed patterns.
    pub const MATCH: Self = Self(2);
    /// Literal resource-name pattern.
    pub const LITERAL: Self = Self(3);
    /// Resource-name prefix pattern.
    pub const PREFIXED: Self = Self(4);

    /// Preserves one exact signed Kafka code, including future values.
    pub const fn from_code(code: i8) -> Self {
        Self(code)
    }

    /// Returns the exact signed Kafka code.
    pub const fn code(self) -> i8 {
        self.0
    }

    /// Reports whether this code names a concrete rather than filter-only pattern.
    pub const fn is_valid_for_binding(self) -> bool {
        self.code() > Self::MATCH.code()
    }

    /// Reports whether this code can select patterns in a filter.
    pub const fn is_valid_for_filter(self) -> bool {
        self.code() > Self::UNKNOWN.code()
    }
}
