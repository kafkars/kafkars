//! Immediate bounded-admission errors for Admin `DescribeAcls`.

use core::fmt;

use kafka_client_core::DescribeAclsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeAclsAdmissionErrorKind {
    /// The ACL filter is invalid.
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

/// Immediate definitely-unsent Admin `DescribeAcls` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeAclsAdmissionError {
    kind: DescribeAclsAdmissionErrorKind,
}

impl DescribeAclsAdmissionError {
    pub(crate) const fn new(kind: DescribeAclsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DescribeAclsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DescribeAclsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeAcls admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for DescribeAclsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeAclsHostError {
    Machine(DescribeAclsMachineError),
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

impl From<DescribeAclsMachineError> for DescribeAclsHostError {
    fn from(error: DescribeAclsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DescribeAclsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DescribeAclsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeAcls host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for DescribeAclsHostError {}
