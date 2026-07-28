//! Immediate bounded-admission and host errors for Admin `DescribeLogDirs`.

use core::fmt;

use kafka_client_core::AdminDescribeLogDirsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeLogDirsAdmissionErrorKind {
    /// The selected broker batch is invalid.
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

/// Immediate definitely-unsent Admin `DescribeLogDirs` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeLogDirsAdmissionError {
    kind: DescribeLogDirsAdmissionErrorKind,
}

impl DescribeLogDirsAdmissionError {
    pub(crate) const fn new(kind: DescribeLogDirsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DescribeLogDirsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DescribeLogDirsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeLogDirs admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for DescribeLogDirsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeLogDirsHostError {
    Machine(AdminDescribeLogDirsMachineError),
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

impl From<AdminDescribeLogDirsMachineError> for DescribeLogDirsHostError {
    fn from(error: AdminDescribeLogDirsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DescribeLogDirsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DescribeLogDirsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeLogDirs host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for DescribeLogDirsHostError {}
