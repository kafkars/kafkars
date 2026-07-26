//! Closed Fetch request visibility values without raw policy integers.

/// Transactional visibility selected by deterministic consumer policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchIsolation {
    /// Return every application record accepted by the log.
    ReadUncommitted,
    /// Return records below the last stable offset plus abort metadata.
    ReadCommitted,
}

impl FetchIsolation {
    pub(super) const fn wire_value(self) -> i8 {
        match self {
            Self::ReadUncommitted => 0,
            Self::ReadCommitted => 1,
        }
    }
}
