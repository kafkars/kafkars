//! Stable immediate `CreatePartitions` admission failures.

use core::fmt;

/// Stable category for a request that never crossed admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreatePartitionsAdmissionErrorKind {
    /// The request violates deterministic validation.
    InvalidRequest,
    /// The requested timeout cannot become an absolute deadline.
    InvalidDeadline,
    /// The concrete shard is briefly owned by another turn.
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

/// Immediate definitely-unsent `CreatePartitions` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreatePartitionsAdmissionError {
    kind: CreatePartitionsAdmissionErrorKind,
}

impl CreatePartitionsAdmissionError {
    pub(crate) const fn new(kind: CreatePartitionsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> CreatePartitionsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for CreatePartitionsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "CreatePartitions admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for CreatePartitionsAdmissionError {}
