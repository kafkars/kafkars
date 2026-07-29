//! Immediate admission and host errors for Admin `ListShareGroupOffsets`.

use core::fmt;

use kafka_client_core::ListShareGroupOffsetsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request rejected before engine ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListShareGroupOffsetsAdmissionErrorKind {
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

/// Immediate definitely-unsent Admin `ListShareGroupOffsets` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetsAdmissionError {
    kind: ListShareGroupOffsetsAdmissionErrorKind,
}

impl ListShareGroupOffsetsAdmissionError {
    pub(crate) const fn new(kind: ListShareGroupOffsetsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> ListShareGroupOffsetsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for ListShareGroupOffsetsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin ListShareGroupOffsets admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for ListShareGroupOffsetsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListShareGroupOffsetsHostError {
    Machine(ListShareGroupOffsetsMachineError),
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

impl From<ListShareGroupOffsetsMachineError> for ListShareGroupOffsetsHostError {
    fn from(error: ListShareGroupOffsetsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for ListShareGroupOffsetsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for ListShareGroupOffsetsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin ListShareGroupOffsets host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for ListShareGroupOffsetsHostError {}
