//! Immediate admission and retained host-invariant failures for offset deletion.

use core::fmt;

use kafka_client_core::DeleteConsumerGroupOffsetsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteConsumerGroupOffsetsAdmissionErrorKind {
    /// The group or topic-partition batch is invalid.
    InvalidRequest,
    /// The requested timeout cannot become an absolute deadline.
    InvalidDeadline,
    /// The concrete owner is briefly held by another turn.
    Contended,
    /// Admin admission has closed.
    Closed,
    /// The concrete operation owner has no free slot.
    Capacity,
    /// The combined request, scratch, and result envelope cannot be reserved.
    RetainedBytes,
    /// Stable operation identities are exhausted.
    IdentityExhausted,
    /// Terminal completion ownership is unavailable.
    HostUnavailable,
}

/// Immediate definitely-unsent offset-deletion rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteConsumerGroupOffsetsAdmissionError {
    kind: DeleteConsumerGroupOffsetsAdmissionErrorKind,
}

impl DeleteConsumerGroupOffsetsAdmissionError {
    pub(crate) const fn new(kind: DeleteConsumerGroupOffsetsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DeleteConsumerGroupOffsetsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DeleteConsumerGroupOffsetsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DeleteConsumerGroupOffsets admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for DeleteConsumerGroupOffsetsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteConsumerGroupOffsetsHostError {
    Machine(DeleteConsumerGroupOffsetsMachineError),
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

impl From<DeleteConsumerGroupOffsetsMachineError> for DeleteConsumerGroupOffsetsHostError {
    fn from(error: DeleteConsumerGroupOffsetsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DeleteConsumerGroupOffsetsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DeleteConsumerGroupOffsetsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DeleteConsumerGroupOffsets host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for DeleteConsumerGroupOffsetsHostError {}
