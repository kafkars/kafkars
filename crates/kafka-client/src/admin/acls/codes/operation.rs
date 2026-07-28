//! Exact signed Kafka ACL operation codes.

/// Kafka operation controlled by an ACL entry or selected by a filter.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AclOperation(i8);

impl AclOperation {
    /// Unknown operation sentinel.
    pub const UNKNOWN: Self = Self(0);
    /// Filter wildcard for every operation.
    pub const ANY: Self = Self(1);
    /// All operations.
    pub const ALL: Self = Self(2);
    /// Read.
    pub const READ: Self = Self(3);
    /// Write.
    pub const WRITE: Self = Self(4);
    /// Create.
    pub const CREATE: Self = Self(5);
    /// Delete.
    pub const DELETE: Self = Self(6);
    /// Alter.
    pub const ALTER: Self = Self(7);
    /// Describe.
    pub const DESCRIBE: Self = Self(8);
    /// Cluster action.
    pub const CLUSTER_ACTION: Self = Self(9);
    /// Describe configurations.
    pub const DESCRIBE_CONFIGS: Self = Self(10);
    /// Alter configurations.
    pub const ALTER_CONFIGS: Self = Self(11);
    /// Idempotent write.
    pub const IDEMPOTENT_WRITE: Self = Self(12);
    /// Create delegation tokens.
    pub const CREATE_TOKENS: Self = Self(13);
    /// Describe delegation tokens.
    pub const DESCRIBE_TOKENS: Self = Self(14);
    /// Participate in two-phase commit.
    pub const TWO_PHASE_COMMIT: Self = Self(15);

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

    /// Reports whether this code can select operations in a filter.
    pub const fn is_valid_for_filter(self) -> bool {
        self.code() > Self::UNKNOWN.code()
    }
}
