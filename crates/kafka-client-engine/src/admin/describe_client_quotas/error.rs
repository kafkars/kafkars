//! Immediate bounded-admission errors for Admin `DescribeClientQuotas`.

use core::fmt;

use kafka_client_core::DescribeClientQuotasMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeClientQuotasAdmissionErrorKind {
    /// The CLIENT QUOTA filter is invalid.
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

/// Immediate definitely-unsent Admin `DescribeClientQuotas` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeClientQuotasAdmissionError {
    kind: DescribeClientQuotasAdmissionErrorKind,
}

impl DescribeClientQuotasAdmissionError {
    pub(crate) const fn new(kind: DescribeClientQuotasAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DescribeClientQuotasAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DescribeClientQuotasAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeClientQuotas admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for DescribeClientQuotasAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeClientQuotasHostError {
    Machine(DescribeClientQuotasMachineError),
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

impl From<DescribeClientQuotasMachineError> for DescribeClientQuotasHostError {
    fn from(error: DescribeClientQuotasMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DescribeClientQuotasHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DescribeClientQuotasHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeClientQuotas host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for DescribeClientQuotasHostError {}
