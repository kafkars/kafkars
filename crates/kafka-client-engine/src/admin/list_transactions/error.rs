//! Immediate bounded-admission and host errors for Admin `ListTransactions`.

use core::fmt;

use kafka_client_core::AdminListTransactionsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminListTransactionsAdmissionErrorKind {
    /// One filter or request shape is invalid.
    InvalidRequest,
    /// The requested duration cannot produce an absolute deadline.
    InvalidDeadline,
    /// Another bounded host turn currently owns the shard.
    Contended,
    /// Admin admission has closed.
    Closed,
    /// The concrete operation owner has no free slot.
    Capacity,
    /// The complete request and result envelope cannot be reserved.
    RetainedBytes,
    /// Stable operation identities are exhausted.
    IdentityExhausted,
    /// Terminal completion ownership is unavailable.
    HostUnavailable,
}

/// Immediate definitely-unsent Admin `ListTransactions` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminListTransactionsAdmissionError {
    kind: AdminListTransactionsAdmissionErrorKind,
}

impl AdminListTransactionsAdmissionError {
    pub(crate) const fn new(kind: AdminListTransactionsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> AdminListTransactionsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for AdminListTransactionsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin ListTransactions admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AdminListTransactionsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdminListTransactionsHostError {
    Machine(AdminListTransactionsMachineError),
    Completion(CompletionRegistryError),
    UnknownOperation,
    MissingSubmission,
    MissingTerminal,
    SubmissionMismatch,
    InvalidHandoff,
    CallCompletion,
    ByteAccounting,
    Unsettled(usize),
    Wake,
}

impl From<AdminListTransactionsMachineError> for AdminListTransactionsHostError {
    fn from(error: AdminListTransactionsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for AdminListTransactionsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for AdminListTransactionsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin ListTransactions host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for AdminListTransactionsHostError {}
