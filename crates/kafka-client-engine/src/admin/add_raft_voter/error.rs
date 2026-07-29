//! Immediate admission and concrete-host errors for Admin `AddRaftVoter`.

use core::fmt;

use kafka_client_core::AddRaftVoterMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request rejected before engine ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddRaftVoterAdmissionErrorKind {
    /// One cluster, voter, directory, or listener scalar is invalid.
    InvalidRequest,
    /// The requested duration cannot produce a live absolute deadline.
    InvalidDeadline,
    /// Another bounded host turn currently owns the concrete shard.
    Contended,
    /// Admin admission has closed.
    Closed,
    /// The concrete operation owner has no free slot.
    Capacity,
    /// The complete request and terminal envelope cannot be reserved.
    RetainedBytes,
    /// Stable operation identities are exhausted.
    IdentityExhausted,
    /// Terminal completion ownership is unavailable.
    HostUnavailable,
}

/// Immediate definitely-unsent Admin `AddRaftVoter` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AddRaftVoterAdmissionError {
    kind: AddRaftVoterAdmissionErrorKind,
}

impl AddRaftVoterAdmissionError {
    pub(crate) const fn new(kind: AddRaftVoterAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> AddRaftVoterAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for AddRaftVoterAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin AddRaftVoter admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AddRaftVoterAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AddRaftVoterHostError {
    Machine(AddRaftVoterMachineError),
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

impl From<AddRaftVoterMachineError> for AddRaftVoterHostError {
    fn from(error: AddRaftVoterMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for AddRaftVoterHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for AddRaftVoterHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin AddRaftVoter host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for AddRaftVoterHostError {}
