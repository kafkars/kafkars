//! Immediate bounded-admission and host errors for Admin `FenceProducers`.

use core::fmt;

use kafka_client_core::AdminFenceProducersMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminFenceProducersAdmissionErrorKind {
    /// One transactional-ID or batch shape is invalid.
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

/// Immediate definitely-unsent Admin `FenceProducers` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminFenceProducersAdmissionError {
    kind: AdminFenceProducersAdmissionErrorKind,
}

impl AdminFenceProducersAdmissionError {
    pub(crate) const fn new(kind: AdminFenceProducersAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> AdminFenceProducersAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for AdminFenceProducersAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin FenceProducers admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AdminFenceProducersAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdminFenceProducersHostError {
    Machine(AdminFenceProducersMachineError),
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

impl From<AdminFenceProducersMachineError> for AdminFenceProducersHostError {
    fn from(error: AdminFenceProducersMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for AdminFenceProducersHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for AdminFenceProducersHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin FenceProducers host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for AdminFenceProducersHostError {}
