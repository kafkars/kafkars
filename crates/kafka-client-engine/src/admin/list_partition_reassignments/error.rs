//! Immediate admission and internal invariant failures for reassignment listing.

use core::fmt;

use kafka_client_core::ListPartitionReassignmentsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListPartitionReassignmentsAdmissionErrorKind {
    /// The selected request contains invalid or ambiguous input.
    InvalidRequest,
    /// The supplied timeout cannot become an absolute deadline.
    InvalidDeadline,
    /// The concrete owner is briefly held by another turn.
    Contended,
    /// Admin admission has closed.
    Closed,
    /// The concrete owner has no free operation slot.
    Capacity,
    /// The complete request/result envelope cannot be reserved.
    RetainedBytes,
    /// Stable operation identities are exhausted.
    IdentityExhausted,
    /// Terminal completion ownership is unavailable.
    HostUnavailable,
}

/// Immediate definitely-unsent reassignment-listing rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListPartitionReassignmentsAdmissionError {
    kind: ListPartitionReassignmentsAdmissionErrorKind,
}

impl ListPartitionReassignmentsAdmissionError {
    pub(crate) const fn new(kind: ListPartitionReassignmentsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> ListPartitionReassignmentsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for ListPartitionReassignmentsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ListPartitionReassignments admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for ListPartitionReassignmentsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListPartitionReassignmentsHostError {
    Machine(ListPartitionReassignmentsMachineError),
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

impl From<ListPartitionReassignmentsMachineError> for ListPartitionReassignmentsHostError {
    fn from(error: ListPartitionReassignmentsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for ListPartitionReassignmentsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for ListPartitionReassignmentsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ListPartitionReassignments host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for ListPartitionReassignmentsHostError {}
