//! Immediate bounded-admission failures for resource-generic `LegacyAlterConfigs`.

use core::fmt;

use kafka_client_core::LegacyAlterConfigsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LegacyAlterConfigsAdmissionErrorKind {
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

/// Immediate definitely-unsent `LegacyAlterConfigs` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LegacyAlterConfigsAdmissionError {
    kind: LegacyAlterConfigsAdmissionErrorKind,
}

impl LegacyAlterConfigsAdmissionError {
    pub(crate) const fn new(kind: LegacyAlterConfigsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> LegacyAlterConfigsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for LegacyAlterConfigsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "LegacyAlterConfigs admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for LegacyAlterConfigsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LegacyAlterConfigsHostError {
    Machine(LegacyAlterConfigsMachineError),
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

impl From<LegacyAlterConfigsMachineError> for LegacyAlterConfigsHostError {
    fn from(error: LegacyAlterConfigsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for LegacyAlterConfigsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for LegacyAlterConfigsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "LegacyAlterConfigs host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for LegacyAlterConfigsHostError {}
