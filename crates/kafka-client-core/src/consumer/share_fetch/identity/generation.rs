//! Explicit nonreused generations fencing `ShareFetch` ownership.

/// Generation fencing one driver-issued broker route receipt.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ShareRouteGeneration(u64);

impl ShareRouteGeneration {
    /// Restores one validated nonzero generation.
    pub const fn try_from_raw(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the first generation.
    pub const fn initial() -> Self {
        Self(1)
    }

    /// Returns the raw generation.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Generation fencing one concrete broker connection.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ShareConnectionGeneration(u64);

impl ShareConnectionGeneration {
    /// Restores one validated nonzero generation.
    pub const fn try_from_raw(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the first generation.
    pub const fn initial() -> Self {
        Self(1)
    }

    /// Returns the raw generation.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Generation fencing the assignment snapshot used by `ShareFetch`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ShareFetchAssignmentGeneration(u64);

impl ShareFetchAssignmentGeneration {
    /// Restores one validated nonzero generation.
    pub const fn try_from_raw(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the first generation.
    pub const fn initial() -> Self {
        Self(1)
    }

    /// Returns the raw generation.
    pub const fn get(self) -> u64 {
        self.0
    }

    pub(in crate::consumer::share_fetch) const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

/// Nonreused local identity of one acquired offset range.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ShareAcquisitionGeneration(u64);

impl ShareAcquisitionGeneration {
    /// Restores one validated nonzero generation.
    pub const fn try_from_raw(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the first generation.
    pub const fn initial() -> Self {
        Self(1)
    }

    /// Returns the raw generation.
    pub const fn get(self) -> u64 {
        self.0
    }

    pub(in crate::consumer::share_fetch) const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}
