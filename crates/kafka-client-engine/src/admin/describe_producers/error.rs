//! Immediate bounded-admission and host errors for Admin `DescribeProducers`.

use core::fmt;

use kafka_client_core::AdminDescribeProducersMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeProducersAdmissionErrorKind {
    /// One topic, partition, duplicate, or batch shape is invalid.
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

/// Immediate definitely-unsent Admin `DescribeProducers` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminDescribeProducersAdmissionError {
    kind: AdminDescribeProducersAdmissionErrorKind,
}

impl AdminDescribeProducersAdmissionError {
    pub(crate) const fn new(kind: AdminDescribeProducersAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> AdminDescribeProducersAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for AdminDescribeProducersAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeProducers admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AdminDescribeProducersAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdminDescribeProducersHostError {
    Machine(AdminDescribeProducersMachineError),
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

impl From<AdminDescribeProducersMachineError> for AdminDescribeProducersHostError {
    fn from(error: AdminDescribeProducersMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for AdminDescribeProducersHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for AdminDescribeProducersHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeProducers host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for AdminDescribeProducersHostError {}
