//! Immediate bounded-admission errors for Admin `AlterReplicaLogDirs`.

use core::fmt;

use kafka_client_core::AlterReplicaLogDirsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterReplicaLogDirsAdmissionErrorKind {
    /// The caller-ordered assignment batch is invalid.
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

/// Immediate definitely-unsent Admin `AlterReplicaLogDirs` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlterReplicaLogDirsAdmissionError {
    kind: AlterReplicaLogDirsAdmissionErrorKind,
}

impl AlterReplicaLogDirsAdmissionError {
    pub(crate) const fn new(kind: AlterReplicaLogDirsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> AlterReplicaLogDirsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for AlterReplicaLogDirsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin AlterReplicaLogDirs admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AlterReplicaLogDirsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterReplicaLogDirsHostError {
    Machine(AlterReplicaLogDirsMachineError),
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

impl From<AlterReplicaLogDirsMachineError> for AlterReplicaLogDirsHostError {
    fn from(error: AlterReplicaLogDirsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for AlterReplicaLogDirsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for AlterReplicaLogDirsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin AlterReplicaLogDirs host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for AlterReplicaLogDirsHostError {}
