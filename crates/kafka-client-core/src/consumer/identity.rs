//! Direct-consumer lifecycle identities and monotonic generation counters.

/// Core-owned identity of the sole accepted direct-consumer close.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AssignedConsumerCloseId(u64);

impl AssignedConsumerCloseId {
    pub(crate) const fn initial() -> Self {
        Self(1)
    }

    #[cfg(test)]
    pub(super) const fn from_raw_for_test(value: u64) -> Self {
        Self(value)
    }

    /// Returns the deterministic close identity.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Monotonic identity of one complete direct assignment.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AssignmentEpoch(u64);

impl AssignmentEpoch {
    pub(crate) const fn initial() -> Self {
        Self(1)
    }

    pub(crate) const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the deterministic epoch value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Generation fencing position replacement within one assigned partition.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PositionEpoch(u64);

impl PositionEpoch {
    pub(crate) const fn initial() -> Self {
        Self(1)
    }

    pub(crate) const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[cfg(test)]
    pub(super) const fn try_from_raw_for_test(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the deterministic epoch value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Monotonic identity of one fetch issued for a position epoch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FetchRevision(u64);

impl FetchRevision {
    pub(crate) const fn initial() -> Self {
        Self(1)
    }

    pub(crate) const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[cfg(test)]
    pub(super) const fn try_from_raw_for_test(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the deterministic revision value.
    pub const fn get(self) -> u64 {
        self.0
    }
}
