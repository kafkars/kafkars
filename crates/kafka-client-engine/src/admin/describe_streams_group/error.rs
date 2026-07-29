//! Immediate admission and host errors for Admin `DescribeStreamsGroup`.

use core::fmt;

use kafka_client_core::DescribeStreamsGroupMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request rejected before engine ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeStreamsGroupAdmissionErrorKind {
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

/// Immediate definitely-unsent Admin `DescribeStreamsGroup` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupAdmissionError {
    kind: DescribeStreamsGroupAdmissionErrorKind,
}

impl DescribeStreamsGroupAdmissionError {
    pub(crate) const fn new(kind: DescribeStreamsGroupAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DescribeStreamsGroupAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DescribeStreamsGroupAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeStreamsGroup admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for DescribeStreamsGroupAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeStreamsGroupHostError {
    Machine(DescribeStreamsGroupMachineError),
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

impl From<DescribeStreamsGroupMachineError> for DescribeStreamsGroupHostError {
    fn from(error: DescribeStreamsGroupMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DescribeStreamsGroupHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DescribeStreamsGroupHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeStreamsGroup host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for DescribeStreamsGroupHostError {}
