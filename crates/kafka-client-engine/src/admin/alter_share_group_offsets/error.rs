//! Immediate admission and host errors for Admin `AlterShareGroupOffsets`.

use core::fmt;

use kafka_client_core::AlterShareGroupOffsetsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request rejected before engine ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterShareGroupOffsetsAdmissionErrorKind {
    /// The group or topic selection is invalid.
    InvalidRequest,
    /// The requested duration cannot produce a live absolute deadline.
    InvalidDeadline,
    /// Another bounded host turn currently owns the concrete shard.
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

/// Immediate definitely-unsent Admin `AlterShareGroupOffsets` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlterShareGroupOffsetsAdmissionError {
    kind: AlterShareGroupOffsetsAdmissionErrorKind,
}

impl AlterShareGroupOffsetsAdmissionError {
    pub(crate) const fn new(kind: AlterShareGroupOffsetsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> AlterShareGroupOffsetsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for AlterShareGroupOffsetsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin AlterShareGroupOffsets admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AlterShareGroupOffsetsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterShareGroupOffsetsHostError {
    Machine(AlterShareGroupOffsetsMachineError),
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

impl From<AlterShareGroupOffsetsMachineError> for AlterShareGroupOffsetsHostError {
    fn from(error: AlterShareGroupOffsetsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for AlterShareGroupOffsetsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for AlterShareGroupOffsetsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin AlterShareGroupOffsets host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for AlterShareGroupOffsetsHostError {}
