//! Immediate admission and retained-host errors for delegation-token creation.

use core::fmt;

use kafka_client_core::CreateDelegationTokenMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request rejected before engine ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateDelegationTokenAdmissionErrorKind {
    /// An owner, renewer, lifetime, or request shape is invalid.
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
    /// The complete request and secret-bearing terminal envelope cannot be reserved.
    RetainedBytes,
    /// A pre-admission engine invariant failed.
    HostInvariant,
    /// Stable operation identities are exhausted.
    IdentityExhausted,
    /// Terminal completion ownership is unavailable.
    HostUnavailable,
}

/// Immediate definitely-unsent Admin `CreateDelegationToken` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreateDelegationTokenAdmissionError {
    kind: CreateDelegationTokenAdmissionErrorKind,
}

impl CreateDelegationTokenAdmissionError {
    pub(crate) const fn new(kind: CreateDelegationTokenAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> CreateDelegationTokenAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for CreateDelegationTokenAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin CreateDelegationToken admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for CreateDelegationTokenAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreateDelegationTokenHostError {
    Machine(CreateDelegationTokenMachineError),
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

impl From<CreateDelegationTokenMachineError> for CreateDelegationTokenHostError {
    fn from(error: CreateDelegationTokenMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for CreateDelegationTokenHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for CreateDelegationTokenHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin CreateDelegationToken host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for CreateDelegationTokenHostError {}
