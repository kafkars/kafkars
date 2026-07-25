//! Nonzero catalog identities fencing one group assignment.

/// Stable engine-catalog identity of one consumer group.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GroupId(u64);

impl GroupId {
    /// Restores a validated nonzero group identity.
    pub const fn try_from_raw(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the catalog identity.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Stable engine-catalog identity of one group member.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MemberId(u64);

impl MemberId {
    /// Restores a validated nonzero member identity.
    pub const fn try_from_raw(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the catalog identity.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Core-owned nonzero generation fencing one installed live assignment.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AssignmentGeneration(u64);

impl AssignmentGeneration {
    pub(crate) const fn initial() -> Self {
        Self(1)
    }

    pub(crate) const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Restores a validated nonzero assignment generation.
    pub const fn try_from_raw(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the generation value.
    pub const fn get(self) -> u64 {
        self.0
    }
}
