//! Immediate admission and retained-host errors for delegation-token description.

use core::fmt;

use kafka_client_core::DescribeDelegationTokensMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request rejected before engine ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeDelegationTokensAdmissionErrorKind {
    /// An owner selection or principal is invalid.
    InvalidRequest,
    /// The requested duration cannot produce a live absolute deadline.
    InvalidDeadline,
    /// Synchronous conversion consumed the original deadline.
    DeadlineElapsed,
    /// Another bounded host turn currently owns the concrete shard.
    Contended,
    /// Admin admission has closed.
    Closed,
    /// The concrete operation owner has no free slot.
    Capacity,
    /// The complete request and secret-bearing terminal envelope cannot be reserved.
    RetainedBytes,
    /// A pre-admission engine invariant failed.
    HostInvariant,
    /// Stable operation identities are exhausted.
    IdentityExhausted,
    /// Terminal completion ownership is unavailable.
    HostUnavailable,
}

/// Immediate definitely-unsent Admin `DescribeDelegationTokens` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeDelegationTokensAdmissionError {
    kind: DescribeDelegationTokensAdmissionErrorKind,
}

impl DescribeDelegationTokensAdmissionError {
    pub(crate) const fn new(kind: DescribeDelegationTokensAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DescribeDelegationTokensAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DescribeDelegationTokensAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeDelegationTokens admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for DescribeDelegationTokensAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeDelegationTokensHostError {
    Machine(DescribeDelegationTokensMachineError),
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

impl From<DescribeDelegationTokensMachineError> for DescribeDelegationTokensHostError {
    fn from(error: DescribeDelegationTokensMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DescribeDelegationTokensHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DescribeDelegationTokensHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeDelegationTokens host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for DescribeDelegationTokensHostError {}
