//! Immediate admission and host errors for Admin `DescribeFeatures`.

use core::fmt;

use kafka_client_core::DescribeFeaturesMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request rejected before engine ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeFeaturesAdmissionErrorKind {
    /// The requested duration cannot produce an absolute deadline.
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

/// Immediate definitely-unsent Admin `DescribeFeatures` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeFeaturesAdmissionError {
    kind: DescribeFeaturesAdmissionErrorKind,
}

impl DescribeFeaturesAdmissionError {
    pub(crate) const fn new(kind: DescribeFeaturesAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DescribeFeaturesAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DescribeFeaturesAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeFeatures admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for DescribeFeaturesAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeFeaturesHostError {
    Machine(DescribeFeaturesMachineError),
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

impl From<DescribeFeaturesMachineError> for DescribeFeaturesHostError {
    fn from(error: DescribeFeaturesMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DescribeFeaturesHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DescribeFeaturesHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeFeatures host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for DescribeFeaturesHostError {}
