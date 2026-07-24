//! Stable immediate `DescribeTopics` admission failures.

use core::fmt;

/// Stable category for a request that never crossed description admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeTopicsAdmissionErrorKind {
    /// The request violates deterministic validation.
    InvalidRequest,
    /// The requested timeout cannot become an absolute deadline.
    InvalidDeadline,
    /// The description shard is briefly owned by another turn.
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

/// Immediate definitely-unsent `DescribeTopics` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeTopicsAdmissionError {
    kind: DescribeTopicsAdmissionErrorKind,
}

impl DescribeTopicsAdmissionError {
    pub(crate) const fn new(kind: DescribeTopicsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DescribeTopicsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DescribeTopicsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeTopics admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for DescribeTopicsAdmissionError {}
