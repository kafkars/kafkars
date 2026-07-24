//! Immediate bounded-admission failures for topic `IncrementalAlterConfigs`.

use core::fmt;

use kafka_client_core::IncrementalAlterConfigsMachineError;

use crate::completion::CompletionRegistryError;

/// Definitely-unsent rejection before an incremental configuration operation exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IncrementalAlterConfigsAdmissionErrorKind {
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
