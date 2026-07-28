//! Immediate bounded-admission errors for Admin `DeleteAcls`.

use core::fmt;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteAclsAdmissionErrorKind {
    /// One filter or the caller-ordered batch shape is invalid.
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

/// Immediate definitely-unsent Admin `DeleteAcls` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteAclsAdmissionError {
    kind: DeleteAclsAdmissionErrorKind,
}

impl DeleteAclsAdmissionError {
    pub(crate) const fn new(kind: DeleteAclsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DeleteAclsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DeleteAclsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DeleteAcls admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for DeleteAclsAdmissionError {}
