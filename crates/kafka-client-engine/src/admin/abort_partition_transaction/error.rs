//! Immediate admission and concrete-host errors for a partition transaction abort.

use core::fmt;

use kafka_client_core::AbortPartitionTransactionMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortPartitionTransactionAdmissionErrorKind {
    /// One topic, partition, producer, or coordinator scalar is invalid.
    InvalidRequest,
    /// The requested duration cannot become one absolute deadline.
    InvalidDeadline,
    /// Another bounded host turn briefly owns the concrete shard.
    Contended,
    /// Admin admission has closed.
    Closed,
    /// The concrete operation owner has no free slot.
    Capacity,
    /// The complete request and terminal envelope cannot be reserved.
    RetainedBytes,
    /// Stable operation identities are exhausted.
    IdentityExhausted,
    /// Terminal completion ownership is unavailable.
    HostUnavailable,
}

/// Immediate definitely-unsent partition transaction-abort rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AbortPartitionTransactionAdmissionError {
    kind: AbortPartitionTransactionAdmissionErrorKind,
}

impl AbortPartitionTransactionAdmissionError {
    pub(crate) const fn new(kind: AbortPartitionTransactionAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> AbortPartitionTransactionAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for AbortPartitionTransactionAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin partition transaction-abort admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AbortPartitionTransactionAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AbortPartitionTransactionHostError {
    Machine(AbortPartitionTransactionMachineError),
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

impl From<AbortPartitionTransactionMachineError> for AbortPartitionTransactionHostError {
    fn from(error: AbortPartitionTransactionMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for AbortPartitionTransactionHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for AbortPartitionTransactionHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin partition transaction-abort host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for AbortPartitionTransactionHostError {}
