//! Stable immediate `DescribeCluster` admission failures.

use core::fmt;

/// Stable category for a call that never crossed admin admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeClusterAdmissionErrorKind {
    /// The requested timeout is zero or cannot become an absolute deadline.
    InvalidDeadline,
    /// The concrete owner is briefly held by another caller or host turn.
    Contended,
    /// Admin admission has closed.
    Closed,
    /// Bounded terminal capacity is full.
    Capacity,
    /// Bounded retained-result capacity is full.
    RetainedBytes,
    /// Stable operation identities are exhausted.
    IdentityExhausted,
    /// The host completion mechanism is unavailable.
    HostUnavailable,
}

/// Immediate definitely-unsent `DescribeCluster` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeClusterAdmissionError {
    kind: DescribeClusterAdmissionErrorKind,
}

impl DescribeClusterAdmissionError {
    pub(crate) const fn new(kind: DescribeClusterAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DescribeClusterAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DescribeClusterAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeCluster admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for DescribeClusterAdmissionError {}
