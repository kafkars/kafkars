//! Immediate admission and retained-host errors for delegation-token renewal.

use core::fmt;

use kafka_client_core::RenewDelegationTokenMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request rejected before engine ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenewDelegationTokenAdmissionErrorKind {
    /// The HMAC, renewal period, or request shape is invalid.
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

/// Immediate definitely-unsent Admin `RenewDelegationToken` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RenewDelegationTokenAdmissionError {
    kind: RenewDelegationTokenAdmissionErrorKind,
}

impl RenewDelegationTokenAdmissionError {
    pub(crate) const fn new(kind: RenewDelegationTokenAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> RenewDelegationTokenAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for RenewDelegationTokenAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin RenewDelegationToken admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for RenewDelegationTokenAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RenewDelegationTokenHostError {
    Machine(RenewDelegationTokenMachineError),
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

impl From<RenewDelegationTokenMachineError> for RenewDelegationTokenHostError {
    fn from(error: RenewDelegationTokenMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for RenewDelegationTokenHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for RenewDelegationTokenHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin RenewDelegationToken host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for RenewDelegationTokenHostError {}
