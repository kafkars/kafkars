//! Stable public rejection categories for transactional lifecycle control.

use core::fmt;

use super::TransactionToken;

/// Stable reason a transactional lifecycle control operation was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionControlErrorKind {
    /// The requested timeout cannot form one positive absolute deadline.
    InvalidDeadline,
    /// The bounded transaction shard is currently owned by another caller.
    Contended,
    /// Shutdown has closed transaction control.
    Closed,
    /// The supplied initialized owner is no longer installed.
    StaleOwner,
    /// The initialized owner already has an active transaction.
    AlreadyActive,
    /// The initialized owner has no active transaction.
    NotActive,
    /// The supplied transaction token does not name the active epoch.
    StaleTransaction,
    /// Accepted transactional operations still require settlement.
    OutstandingOperations,
    /// Prior execution uncertainty requires abort rather than commit.
    AbortRequired,
    /// Commit, abort, or owner-loss cleanup is already in progress.
    EndInProgress,
    /// Transaction execution has permanently fenced this owner.
    Fenced,
    /// Bounded completion capacity is unavailable.
    Backpressure,
    /// A nonreused internal identity domain is exhausted.
    IdentityExhausted,
    /// An internal transaction invariant made the host unavailable.
    HostUnavailable,
}

/// Rejection of a transaction begin operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionControlError {
    kind: TransactionControlErrorKind,
}

impl TransactionControlError {
    pub(super) const fn new(kind: TransactionControlErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> TransactionControlErrorKind {
        self.kind
    }
}

impl fmt::Display for TransactionControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "transaction control rejected: {:?}", self.kind)
    }
}

impl std::error::Error for TransactionControlError {}

/// Rejected commit or abort retaining the exact active transaction token.
#[must_use = "transaction end rejection retains the active transaction token"]
pub struct TransactionEndAdmissionError<'owner> {
    kind: TransactionControlErrorKind,
    transaction: TransactionToken<'owner>,
}

impl<'owner> TransactionEndAdmissionError<'owner> {
    pub(super) const fn new(
        kind: TransactionControlErrorKind,
        transaction: TransactionToken<'owner>,
    ) -> Self {
        Self { kind, transaction }
    }

    /// Returns the stable rejection category.
    pub const fn kind(&self) -> TransactionControlErrorKind {
        self.kind
    }

    /// Returns the exact active transaction token rejected before driver ownership.
    pub fn into_transaction(self) -> TransactionToken<'owner> {
        self.transaction
    }
}

impl fmt::Debug for TransactionEndAdmissionError<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionEndAdmissionError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for TransactionEndAdmissionError<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "transaction end rejected: {:?}", self.kind)
    }
}

impl std::error::Error for TransactionEndAdmissionError<'_> {}
