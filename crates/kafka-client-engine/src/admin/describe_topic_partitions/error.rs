//! Immediate admission and host errors for Admin `DescribeTopicPartitions`.

use core::fmt;

use kafka_client_core::DescribeTopicPartitionsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request rejected before engine ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeTopicPartitionsAdmissionErrorKind {
    /// The topic selection, limit, or cursor is invalid.
    InvalidRequest,
    /// The requested duration cannot produce an absolute deadline.
    InvalidDeadline,
    /// Another bounded host turn currently owns the shard.
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

/// Immediate definitely-unsent Admin `DescribeTopicPartitions` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminDescribeTopicPartitionsAdmissionError {
    kind: AdminDescribeTopicPartitionsAdmissionErrorKind,
}

impl AdminDescribeTopicPartitionsAdmissionError {
    pub(crate) const fn new(kind: AdminDescribeTopicPartitionsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> AdminDescribeTopicPartitionsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for AdminDescribeTopicPartitionsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeTopicPartitions admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AdminDescribeTopicPartitionsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdminDescribeTopicPartitionsHostError {
    Machine(DescribeTopicPartitionsMachineError),
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

impl From<DescribeTopicPartitionsMachineError> for AdminDescribeTopicPartitionsHostError {
    fn from(error: DescribeTopicPartitionsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for AdminDescribeTopicPartitionsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for AdminDescribeTopicPartitionsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeTopicPartitions host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for AdminDescribeTopicPartitionsHostError {}
