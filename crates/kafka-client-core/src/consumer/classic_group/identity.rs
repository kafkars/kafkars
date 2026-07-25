//! Directional identities fencing one classic Join and Sync membership cycle.

/// Nonreused identity of one Join and Sync attempt.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MembershipCycle(u64);

impl MembershipCycle {
    /// Returns the first membership-cycle identity.
    pub const fn initial() -> Self {
        Self(1)
    }

    /// Advances without wrapping or reusing a prior cycle.
    pub const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Restores one validated nonzero cycle.
    pub const fn try_from_raw(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the deterministic cycle value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Engine correlation slot for one member in a normalized Join response.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JoinedMemberSlot(u32);

impl JoinedMemberSlot {
    /// Restores one validated nonzero response slot.
    pub const fn try_from_raw(value: u32) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the engine correlation slot.
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// One-based ordering rank derived from Kafka's opaque member identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MemberRank(u32);

impl MemberRank {
    /// Restores one validated nonzero ordering rank.
    pub const fn try_from_raw(value: u32) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the one-based ordering rank.
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Signed Kafka generation returned by classic `JoinGroup`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClassicGeneration(i32);

impl ClassicGeneration {
    /// Accepts Kafka's nonnegative classic generation domain.
    pub const fn try_from_raw(value: i32) -> Option<Self> {
        if value < 0 { None } else { Some(Self(value)) }
    }

    /// Returns the exact Kafka generation.
    pub const fn get(self) -> i32 {
        self.0
    }
}
