//! Stable immediate admission failures for resource-generic `DescribeConfigs`.

use core::fmt;

/// Stable category for a request that never crossed description admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeConfigsAdmissionErrorKind {
    /// The request violates deterministic validation.
    InvalidRequest,
    /// This execution slice cannot honestly route one requested resource.
    UnsupportedResource,
    /// The timeout cannot become an absolute deadline.
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

/// Immediate definitely-unsent `DescribeConfigs` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeConfigsAdmissionError {
    kind: DescribeConfigsAdmissionErrorKind,
}

impl DescribeConfigsAdmissionError {
    pub(crate) const fn new(kind: DescribeConfigsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DescribeConfigsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DescribeConfigsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeConfigs admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for DescribeConfigsAdmissionError {}
