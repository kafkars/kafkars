//! Immediate admission and retained-host errors for delegation-token expiration.

use core::fmt;

use kafka_client_core::ExpireDelegationTokenMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request rejected before engine ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpireDelegationTokenAdmissionErrorKind {
    /// The HMAC, expiration period, or request shape is invalid.
    InvalidRequest,
    /// The requested duration cannot produce a live absolute deadline.
    InvalidDeadline,
    /// Synchronous request preparation consumed the original deadline.
    DeadlineElapsed,
    /// Another bounded host turn currently owns the concrete shard.
    Contended,
    /// Admin admission has closed.
    Closed,
    /// The concrete operation owner has no free slot.
    Capacity,
    /// The complete secret-bearing request-result envelope cannot be reserved.
    RetainedBytes,
    /// A pre-admission engine invariant failed.
    HostInvariant,
    /// Stable operation identities are exhausted.
    IdentityExhausted,
    /// Terminal completion ownership is unavailable.
    HostUnavailable,
}

/// Immediate definitely-unsent Admin `ExpireDelegationToken` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpireDelegationTokenAdmissionError {
    kind: ExpireDelegationTokenAdmissionErrorKind,
}

impl ExpireDelegationTokenAdmissionError {
    pub(crate) const fn new(kind: ExpireDelegationTokenAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> ExpireDelegationTokenAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for ExpireDelegationTokenAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin ExpireDelegationToken admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for ExpireDelegationTokenAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExpireDelegationTokenHostError {
    Machine(ExpireDelegationTokenMachineError),
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

impl From<ExpireDelegationTokenMachineError> for ExpireDelegationTokenHostError {
    fn from(error: ExpireDelegationTokenMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for ExpireDelegationTokenHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for ExpireDelegationTokenHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin ExpireDelegationToken host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for ExpireDelegationTokenHostError {}
