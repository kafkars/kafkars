//! Immediate bounded-admission and host errors for Admin `DescribeTransactions`.

use core::fmt;

use kafka_client_core::AdminDescribeTransactionsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeTransactionsAdmissionErrorKind {
    /// One transactional-ID or batch shape is invalid.
    InvalidRequest,
    /// The requested duration cannot become one absolute deadline.
    InvalidDeadline,
    /// Another bounded host turn briefly owns the concrete shard.
    Contended,
    /// Admin admission has closed.
    Closed,
    /// The concrete operation owner has no free slot.
    Capacity,
    /// The complete request, scratch, and terminal envelope cannot be reserved.
    RetainedBytes,
    /// Stable operation identities are exhausted.
    IdentityExhausted,
    /// Terminal completion ownership is unavailable.
    HostUnavailable,
}

/// Immediate definitely-unsent Admin `DescribeTransactions` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminDescribeTransactionsAdmissionError {
    kind: AdminDescribeTransactionsAdmissionErrorKind,
}

impl AdminDescribeTransactionsAdmissionError {
    pub(crate) const fn new(kind: AdminDescribeTransactionsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> AdminDescribeTransactionsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for AdminDescribeTransactionsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeTransactions admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AdminDescribeTransactionsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdminDescribeTransactionsHostError {
    Machine(AdminDescribeTransactionsMachineError),
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

impl From<AdminDescribeTransactionsMachineError> for AdminDescribeTransactionsHostError {
    fn from(error: AdminDescribeTransactionsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for AdminDescribeTransactionsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for AdminDescribeTransactionsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeTransactions host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for AdminDescribeTransactionsHostError {}
