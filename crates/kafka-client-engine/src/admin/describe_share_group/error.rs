//! Immediate admission and host errors for Admin `DescribeShareGroup`.

use core::fmt;

use kafka_client_core::DescribeShareGroupMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request rejected before engine ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeShareGroupAdmissionErrorKind {
    /// The group or topic selection is invalid.
    InvalidRequest,
    /// The requested duration cannot produce a live absolute deadline.
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

/// Immediate definitely-unsent Admin `DescribeShareGroup` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupAdmissionError {
    kind: DescribeShareGroupAdmissionErrorKind,
}

impl DescribeShareGroupAdmissionError {
    pub(crate) const fn new(kind: DescribeShareGroupAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DescribeShareGroupAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DescribeShareGroupAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeShareGroup admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for DescribeShareGroupAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeShareGroupHostError {
    Machine(DescribeShareGroupMachineError),
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

impl From<DescribeShareGroupMachineError> for DescribeShareGroupHostError {
    fn from(error: DescribeShareGroupMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DescribeShareGroupHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DescribeShareGroupHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeShareGroup host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for DescribeShareGroupHostError {}
