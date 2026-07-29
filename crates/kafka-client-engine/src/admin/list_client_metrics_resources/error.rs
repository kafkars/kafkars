//! Immediate admission and host errors for Admin `ListClientMetricsResources`.

use core::fmt;

use kafka_client_core::ListClientMetricsResourcesMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request rejected before engine ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListClientMetricsResourcesAdmissionErrorKind {
    /// The requested duration cannot produce an absolute deadline.
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

/// Immediate definitely-unsent Admin `ListClientMetricsResources` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListClientMetricsResourcesAdmissionError {
    kind: ListClientMetricsResourcesAdmissionErrorKind,
}

impl ListClientMetricsResourcesAdmissionError {
    pub(crate) const fn new(kind: ListClientMetricsResourcesAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> ListClientMetricsResourcesAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for ListClientMetricsResourcesAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin ListClientMetricsResources admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for ListClientMetricsResourcesAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListClientMetricsResourcesHostError {
    Machine(ListClientMetricsResourcesMachineError),
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

impl From<ListClientMetricsResourcesMachineError> for ListClientMetricsResourcesHostError {
    fn from(error: ListClientMetricsResourcesMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for ListClientMetricsResourcesHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for ListClientMetricsResourcesHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin ListClientMetricsResources host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for ListClientMetricsResourcesHostError {}
