//! Stable public application-record visibility without engine or protocol types.

/// Whether a consumer may observe records from unresolved transactions.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReadIsolation {
    /// Deliver every application record accepted by the partition log.
    #[default]
    ReadUncommitted,
    /// Deliver only nontransactional or committed transactional records.
    ReadCommitted,
}
