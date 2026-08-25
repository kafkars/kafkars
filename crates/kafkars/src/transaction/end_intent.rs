//! Stable commit-or-abort identity attached to transaction-end failures.

/// Intent of one accepted or rejected transaction-end operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionEndIntent {
    /// Make the transaction's writes visible atomically.
    Commit,
    /// Discard the transaction's writes atomically.
    Abort,
}
