//! Immediate bounded-admission errors for Admin `AlterClientQuotas`.

use core::fmt;

use kafka_client_core::AlterClientQuotasMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterClientQuotasAdmissionErrorKind {
    /// The client-quota alteration batch is invalid.
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

/// Immediate definitely-unsent Admin `AlterClientQuotas` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlterClientQuotasAdmissionError {
    kind: AlterClientQuotasAdmissionErrorKind,
}

impl AlterClientQuotasAdmissionError {
    pub(crate) const fn new(kind: AlterClientQuotasAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> AlterClientQuotasAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for AlterClientQuotasAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin AlterClientQuotas admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AlterClientQuotasAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterClientQuotasHostError {
    Machine(AlterClientQuotasMachineError),
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

impl From<AlterClientQuotasMachineError> for AlterClientQuotasHostError {
    fn from(error: AlterClientQuotasMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for AlterClientQuotasHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for AlterClientQuotasHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin AlterClientQuotas host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for AlterClientQuotasHostError {}
