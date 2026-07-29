//! Immediate admission and host errors for Admin `DeleteShareGroupOffsets`.

use core::fmt;

use kafka_client_core::DeleteShareGroupOffsetsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request rejected before engine ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteShareGroupOffsetsAdmissionErrorKind {
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

/// Immediate definitely-unsent Admin `DeleteShareGroupOffsets` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteShareGroupOffsetsAdmissionError {
    kind: DeleteShareGroupOffsetsAdmissionErrorKind,
}

impl DeleteShareGroupOffsetsAdmissionError {
    pub(crate) const fn new(kind: DeleteShareGroupOffsetsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DeleteShareGroupOffsetsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DeleteShareGroupOffsetsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DeleteShareGroupOffsets admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for DeleteShareGroupOffsetsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteShareGroupOffsetsHostError {
    Machine(DeleteShareGroupOffsetsMachineError),
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

impl From<DeleteShareGroupOffsetsMachineError> for DeleteShareGroupOffsetsHostError {
    fn from(error: DeleteShareGroupOffsetsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DeleteShareGroupOffsetsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DeleteShareGroupOffsetsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DeleteShareGroupOffsets host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for DeleteShareGroupOffsetsHostError {}
