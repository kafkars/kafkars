//! Immediate admission and retained host-invariant failures for offset alteration.

use core::fmt;

use kafka_client_core::AlterConsumerGroupOffsetsMachineError;

use crate::completion::CompletionRegistryError;

use super::AlterConsumerGroupOffsetsRequest;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterConsumerGroupOffsetsAdmissionErrorKind {
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

/// Immediate definitely-unsent offset-alteration rejection.
#[derive(Debug)]
pub struct AlterConsumerGroupOffsetsAdmissionError {
    kind: AlterConsumerGroupOffsetsAdmissionErrorKind,
    request: AlterConsumerGroupOffsetsRequest,
}

impl AlterConsumerGroupOffsetsAdmissionError {
    pub(crate) const fn new(
        kind: AlterConsumerGroupOffsetsAdmissionErrorKind,
        request: AlterConsumerGroupOffsetsRequest,
    ) -> Self {
        Self { kind, request }
    }

    /// Returns the stable rejection category.
    pub const fn kind(&self) -> AlterConsumerGroupOffsetsAdmissionErrorKind {
        self.kind
    }

    /// Returns the exact caller-owned request rejected before admission.
    pub fn into_request(self) -> AlterConsumerGroupOffsetsRequest {
        self.request
    }
}

impl fmt::Display for AlterConsumerGroupOffsetsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "AlterConsumerGroupOffsets admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AlterConsumerGroupOffsetsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterConsumerGroupOffsetsHostError {
    Machine(AlterConsumerGroupOffsetsMachineError),
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

impl From<AlterConsumerGroupOffsetsMachineError> for AlterConsumerGroupOffsetsHostError {
    fn from(error: AlterConsumerGroupOffsetsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for AlterConsumerGroupOffsetsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for AlterConsumerGroupOffsetsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "AlterConsumerGroupOffsets host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for AlterConsumerGroupOffsetsHostError {}
