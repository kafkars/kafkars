//! Immediate bounded-admission failures for group-offset listing.

use core::fmt;

use kafka_client_core::ListConsumerGroupOffsetsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListConsumerGroupOffsetsAdmissionErrorKind {
    /// The group identity or request intent is invalid.
    InvalidRequest,
    /// The requested timeout cannot become an absolute deadline.
    InvalidDeadline,
    /// The concrete owner is briefly held by another turn.
    Contended,
    /// Admin admission has closed.
    Closed,
    /// The concrete operation owner has no free slot.
    Capacity,
    /// The combined request and result envelope cannot be reserved.
    RetainedBytes,
    /// Stable operation identities are exhausted.
    IdentityExhausted,
    /// Terminal completion ownership is unavailable.
    HostUnavailable,
}

/// Immediate definitely-unsent consumer-group offset rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListConsumerGroupOffsetsAdmissionError {
    kind: ListConsumerGroupOffsetsAdmissionErrorKind,
}

impl ListConsumerGroupOffsetsAdmissionError {
    pub(crate) const fn new(kind: ListConsumerGroupOffsetsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> ListConsumerGroupOffsetsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for ListConsumerGroupOffsetsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ListConsumerGroupOffsets admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for ListConsumerGroupOffsetsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListConsumerGroupOffsetsHostError {
    Machine(ListConsumerGroupOffsetsMachineError),
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

impl From<ListConsumerGroupOffsetsMachineError> for ListConsumerGroupOffsetsHostError {
    fn from(error: ListConsumerGroupOffsetsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for ListConsumerGroupOffsetsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for ListConsumerGroupOffsetsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ListConsumerGroupOffsets host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for ListConsumerGroupOffsetsHostError {}
