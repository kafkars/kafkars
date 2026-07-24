//! Stable immediate `DeleteTopics` admission failures.

use core::fmt;

/// Stable category for a request that never crossed deletion admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteTopicsAdmissionErrorKind {
    /// The request violates deterministic validation.
    InvalidRequest,
    /// The requested timeout cannot become an absolute deadline.
    InvalidDeadline,
    /// The deletion shard is briefly owned by another turn.
    Contended,
    /// Admin admission has closed.
    Closed,
    /// Bounded terminal capacity is full.
    Capacity,
    /// The request exceeds retained-byte capacity.
    RetainedBytes,
    /// Stable operation identities are exhausted.
    IdentityExhausted,
    /// The host completion mechanism is unavailable.
    HostUnavailable,
}

/// Immediate definitely-unsent `DeleteTopics` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteTopicsAdmissionError {
    kind: DeleteTopicsAdmissionErrorKind,
}

impl DeleteTopicsAdmissionError {
    pub(crate) const fn new(kind: DeleteTopicsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DeleteTopicsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DeleteTopicsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "DeleteTopics admission failed: {:?}", self.kind)
    }
}

impl std::error::Error for DeleteTopicsAdmissionError {}
