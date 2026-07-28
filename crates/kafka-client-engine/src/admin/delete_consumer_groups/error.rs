//! Immediate bounded-admission and host errors for Admin `DeleteConsumerGroups`.

use core::fmt;

use kafka_client_core::DeleteConsumerGroupsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteConsumerGroupsAdmissionErrorKind {
    /// One group identifier or batch shape is invalid.
    InvalidRequest,
    /// The requested duration cannot become one absolute deadline.
    InvalidDeadline,
    /// Another bounded host turn briefly owns the concrete shard.
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

/// Immediate definitely-unsent Admin `DeleteConsumerGroups` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteConsumerGroupsAdmissionError {
    kind: DeleteConsumerGroupsAdmissionErrorKind,
}

impl DeleteConsumerGroupsAdmissionError {
    pub(crate) const fn new(kind: DeleteConsumerGroupsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DeleteConsumerGroupsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DeleteConsumerGroupsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DeleteConsumerGroups admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for DeleteConsumerGroupsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteConsumerGroupsHostError {
    Machine(DeleteConsumerGroupsMachineError),
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

impl From<DeleteConsumerGroupsMachineError> for DeleteConsumerGroupsHostError {
    fn from(error: DeleteConsumerGroupsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DeleteConsumerGroupsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DeleteConsumerGroupsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DeleteConsumerGroups host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for DeleteConsumerGroupsHostError {}
