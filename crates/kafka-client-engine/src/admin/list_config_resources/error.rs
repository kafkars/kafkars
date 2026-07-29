//! Immediate admission and host errors for Admin `ListConfigResources`.

use core::fmt;

use kafka_client_core::ListConfigResourcesMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request rejected before engine ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListConfigResourcesAdmissionErrorKind {
    /// The resource-type selection is invalid.
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

/// Immediate definitely-unsent Admin `ListConfigResources` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListConfigResourcesAdmissionError {
    kind: ListConfigResourcesAdmissionErrorKind,
}

impl ListConfigResourcesAdmissionError {
    pub(crate) const fn new(kind: ListConfigResourcesAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> ListConfigResourcesAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for ListConfigResourcesAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin ListConfigResources admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for ListConfigResourcesAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListConfigResourcesHostError {
    Machine(ListConfigResourcesMachineError),
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

impl From<ListConfigResourcesMachineError> for ListConfigResourcesHostError {
    fn from(error: ListConfigResourcesMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for ListConfigResourcesHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for ListConfigResourcesHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin ListConfigResources host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for ListConfigResourcesHostError {}
