//! Immediate bounded-admission errors for Admin `CreateAcls`.

use core::fmt;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateAclsAdmissionErrorKind {
    /// One binding or the caller-ordered batch shape is invalid.
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

/// Immediate definitely-unsent Admin `CreateAcls` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreateAclsAdmissionError {
    kind: CreateAclsAdmissionErrorKind,
}

impl CreateAclsAdmissionError {
    pub(crate) const fn new(kind: CreateAclsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> CreateAclsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for CreateAclsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin CreateAcls admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for CreateAclsAdmissionError {}
