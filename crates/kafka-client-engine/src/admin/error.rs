//! Stable immediate `CreateTopics` admission failures.

use core::fmt;

/// Stable category for a request that never crossed admin admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateTopicsAdmissionErrorKind {
    /// The request violates deterministic `CreateTopics` validation.
    InvalidRequest,
    /// The requested timeout is zero or cannot become an absolute deadline.
    InvalidDeadline,
    /// The concrete admin shard is briefly owned by another caller or host turn.
    Contended,
    /// Admin admission has closed.
    Closed,
    /// Bounded operation-completion capacity is full.
    Capacity,
    /// The request exceeds the bounded retained-byte budget.
    RetainedBytes,
    /// Stable operation identities are exhausted.
    IdentityExhausted,
    /// The host's completion mechanism is unavailable.
    HostUnavailable,
}

/// Immediate definitely-unsent `CreateTopics` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreateTopicsAdmissionError {
    kind: CreateTopicsAdmissionErrorKind,
}

impl CreateTopicsAdmissionError {
    pub(crate) const fn new(kind: CreateTopicsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> CreateTopicsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for CreateTopicsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CreateTopics admission failed: {:?}", self.kind)
    }
}

impl std::error::Error for CreateTopicsAdmissionError {}
