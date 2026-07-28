//! Immediate bounded-admission errors for Admin `DescribeUserScramCredentials`.

use core::fmt;

use kafka_client_core::DescribeUserScramCredentialsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeUserScramCredentialsAdmissionErrorKind {
    /// The optional SCRAM user selection is invalid.
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

/// Immediate definitely-unsent Admin `DescribeUserScramCredentials` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeUserScramCredentialsAdmissionError {
    kind: DescribeUserScramCredentialsAdmissionErrorKind,
}

impl DescribeUserScramCredentialsAdmissionError {
    pub(crate) const fn new(kind: DescribeUserScramCredentialsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DescribeUserScramCredentialsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DescribeUserScramCredentialsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeUserScramCredentials admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for DescribeUserScramCredentialsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeUserScramCredentialsHostError {
    Machine(DescribeUserScramCredentialsMachineError),
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

impl From<DescribeUserScramCredentialsMachineError> for DescribeUserScramCredentialsHostError {
    fn from(error: DescribeUserScramCredentialsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DescribeUserScramCredentialsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DescribeUserScramCredentialsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeUserScramCredentials host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for DescribeUserScramCredentialsHostError {}
