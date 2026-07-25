//! Immediate bounded-admission failures for topic `IncrementalAlterConfigs`.

use core::fmt;

use kafka_client_core::IncrementalAlterConfigsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncrementalAlterConfigsAdmissionErrorKind {
    /// The request violates deterministic validation.
    InvalidRequest,
    /// The requested timeout cannot become an absolute deadline.
    InvalidDeadline,
    /// The concrete operation vector has no free slot.
    Capacity,
    /// Admin admission has closed.
    Closed,
    /// The concrete shard is briefly owned by another turn.
    Contended,
    /// Stable operation identities are exhausted.
    IdentityExhausted,
    /// The completion registry cannot reserve terminal ownership.
    HostUnavailable,
    /// The request and terminal result exceed retained-byte capacity.
    RetainedBytes,
}

/// Immediate definitely-unsent `IncrementalAlterConfigs` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IncrementalAlterConfigsAdmissionError {
    kind: IncrementalAlterConfigsAdmissionErrorKind,
}

impl IncrementalAlterConfigsAdmissionError {
    pub(crate) const fn new(kind: IncrementalAlterConfigsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> IncrementalAlterConfigsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for IncrementalAlterConfigsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "IncrementalAlterConfigs admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for IncrementalAlterConfigsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IncrementalAlterConfigsHostError {
    Machine(IncrementalAlterConfigsMachineError),
    Completion(CompletionRegistryError),
    UnknownOperation,
    MissingSubmission,
    MissingTerminal,
    SubmissionMismatch,
    InvalidHandoff,
    ByteAccounting,
    Unsettled(usize),
    Wake,
}

impl From<IncrementalAlterConfigsMachineError> for IncrementalAlterConfigsHostError {
    fn from(error: IncrementalAlterConfigsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for IncrementalAlterConfigsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for IncrementalAlterConfigsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "IncrementalAlterConfigs host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for IncrementalAlterConfigsHostError {}
