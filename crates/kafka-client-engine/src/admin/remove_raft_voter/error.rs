//! Immediate admission and host errors for Admin `RemoveRaftVoter`.

use core::fmt;

use kafka_client_core::RemoveRaftVoterMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request rejected before engine ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveRaftVoterAdmissionErrorKind {
    /// The cluster or voter identity is invalid.
    InvalidRequest,
    /// The requested duration cannot produce a live absolute deadline.
    InvalidDeadline,
    /// Another bounded host turn currently owns the concrete shard.
    Contended,
    /// Admin admission has closed.
    Closed,
    /// The concrete operation owner has no free slot.
    Capacity,
    /// The request and terminal envelope cannot be reserved.
    RetainedBytes,
    /// Stable operation identities are exhausted.
    IdentityExhausted,
    /// Terminal completion ownership is unavailable.
    HostUnavailable,
}

/// Immediate definitely-unsent Admin `RemoveRaftVoter` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoveRaftVoterAdmissionError {
    kind: RemoveRaftVoterAdmissionErrorKind,
}

impl RemoveRaftVoterAdmissionError {
    pub(crate) const fn new(kind: RemoveRaftVoterAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> RemoveRaftVoterAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for RemoveRaftVoterAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin RemoveRaftVoter admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for RemoveRaftVoterAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RemoveRaftVoterHostError {
    Machine(RemoveRaftVoterMachineError),
    Completion(CompletionRegistryError),
    UnknownOperation,
    MissingSubmission,
    MissingTerminal,
    SubmissionMismatch,
    InvalidHandoff,
    CallCompletion,
    DriverMissing,
    ByteAccounting,
    Unsettled(usize),
    Wake,
}

impl From<RemoveRaftVoterMachineError> for RemoveRaftVoterHostError {
    fn from(error: RemoveRaftVoterMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for RemoveRaftVoterHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for RemoveRaftVoterHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin RemoveRaftVoter host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for RemoveRaftVoterHostError {}
