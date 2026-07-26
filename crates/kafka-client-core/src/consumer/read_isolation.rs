//! Immutable application-record visibility policy for one consumer machine.

/// Whether a consumer may observe records from unresolved transactions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadIsolation {
    /// Deliver every application record accepted by the partition log.
    ReadUncommitted,
    /// Deliver only nontransactional or committed transactional records.
    ReadCommitted,
}

impl Default for ReadIsolation {
    fn default() -> Self {
        Self::ReadUncommitted
    }
}
