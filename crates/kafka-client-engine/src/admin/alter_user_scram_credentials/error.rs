//! Immediate admission and retained-host errors for SCRAM credential alteration.

use core::fmt;

use kafka_client_core::AlterUserScramCredentialsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterUserScramCredentialsAdmissionErrorKind {
    /// The caller-owned request is structurally or semantically invalid.
    InvalidRequest,
    /// The requested duration cannot become one absolute deadline.
    InvalidDeadline,
    /// Synchronous preparation consumed the original public deadline.
    DeadlineElapsed,
    /// Another bounded host turn briefly owns the concrete shard.
    Contended,
    /// Admin admission has closed.
    Closed,
    /// The concrete operation owner has no free slot.
    Capacity,
    /// The request, scratch, and terminal envelope cannot be reserved.
    RetainedBytes,
    /// Cryptographic request preparation could not complete.
    Preparation,
    /// A pre-admission engine ownership invariant failed.
    HostInvariant,
    /// Stable operation identities are exhausted.
    IdentityExhausted,
    /// Terminal completion ownership is unavailable.
    HostUnavailable,
}

/// Immediate definitely-unsent SCRAM credential-alteration rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlterUserScramCredentialsAdmissionError {
    kind: AlterUserScramCredentialsAdmissionErrorKind,
}

impl AlterUserScramCredentialsAdmissionError {
    pub(crate) const fn new(kind: AlterUserScramCredentialsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> AlterUserScramCredentialsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for AlterUserScramCredentialsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin AlterUserScramCredentials admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AlterUserScramCredentialsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterUserScramCredentialsHostError {
    Machine(AlterUserScramCredentialsMachineError),
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

impl From<AlterUserScramCredentialsMachineError> for AlterUserScramCredentialsHostError {
    fn from(error: AlterUserScramCredentialsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for AlterUserScramCredentialsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for AlterUserScramCredentialsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin AlterUserScramCredentials host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for AlterUserScramCredentialsHostError {}
