//! Immediate bounded-admission and host errors for Admin `DescribeReplicaLogDirs`.

use core::fmt;

use kafka_client_core::DescribeReplicaLogDirsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeReplicaLogDirsAdmissionErrorKind {
    /// The selected replica batch is invalid.
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

/// Immediate definitely-unsent Admin `DescribeReplicaLogDirs` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeReplicaLogDirsAdmissionError {
    kind: DescribeReplicaLogDirsAdmissionErrorKind,
}

impl DescribeReplicaLogDirsAdmissionError {
    pub(crate) const fn new(kind: DescribeReplicaLogDirsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DescribeReplicaLogDirsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DescribeReplicaLogDirsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeReplicaLogDirs admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for DescribeReplicaLogDirsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeReplicaLogDirsHostError {
    Machine(DescribeReplicaLogDirsMachineError),
    Completion(CompletionRegistryError),
    UnknownOperation,
    MissingSubmission,
    MissingReplicas,
    MissingTerminal,
    SubmissionMismatch,
    InvalidHandoff,
    CallCompletion,
    ByteAccounting,
    Unsettled(usize),
    Wake,
}

impl From<DescribeReplicaLogDirsMachineError> for DescribeReplicaLogDirsHostError {
    fn from(error: DescribeReplicaLogDirsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DescribeReplicaLogDirsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DescribeReplicaLogDirsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeReplicaLogDirs host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for DescribeReplicaLogDirsHostError {}
