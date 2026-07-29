//! Immediate bounded-admission errors for Admin `DescribeMetadataQuorum`.

use core::fmt;

use kafka_client_core::DescribeMetadataQuorumMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeMetadataQuorumAdmissionErrorKind {
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

/// Immediate definitely-unsent Admin `DescribeMetadataQuorum` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeMetadataQuorumAdmissionError {
    kind: DescribeMetadataQuorumAdmissionErrorKind,
}

impl DescribeMetadataQuorumAdmissionError {
    pub(crate) const fn new(kind: DescribeMetadataQuorumAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DescribeMetadataQuorumAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DescribeMetadataQuorumAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeMetadataQuorum admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for DescribeMetadataQuorumAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeMetadataQuorumHostError {
    Machine(DescribeMetadataQuorumMachineError),
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

impl From<DescribeMetadataQuorumMachineError> for DescribeMetadataQuorumHostError {
    fn from(error: DescribeMetadataQuorumMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DescribeMetadataQuorumHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DescribeMetadataQuorumHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeMetadataQuorum host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for DescribeMetadataQuorumHostError {}
