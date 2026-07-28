//! Admission and retained host failures for reassignment alteration.

use core::fmt;

use kafka_client_core::AlterPartitionReassignmentsMachineError;

use crate::completion::CompletionRegistryError;

use super::AlterPartitionReassignmentsRequest;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterPartitionReassignmentsAdmissionErrorKind {
    /// Request contents failed deterministic validation.
    InvalidRequest,
    /// Timeout conversion failed or the supplied timeout was zero.
    InvalidDeadline,
    /// The bounded owner was temporarily contended.
    Contended,
    /// Admin admission has closed.
    Closed,
    /// The bounded operation registry is full.
    Capacity,
    /// Retaining the request would exceed the configured byte budget.
    RetainedBytes,
    /// The operation identity space is exhausted.
    IdentityExhausted,
    /// The operation owner is unavailable.
    HostUnavailable,
}

/// Immediate definitely-unsent reassignment rejection.
#[derive(Debug)]
pub struct AlterPartitionReassignmentsAdmissionError {
    kind: AlterPartitionReassignmentsAdmissionErrorKind,
    request: AlterPartitionReassignmentsRequest,
}

impl AlterPartitionReassignmentsAdmissionError {
    pub(crate) const fn new(
        kind: AlterPartitionReassignmentsAdmissionErrorKind,
        request: AlterPartitionReassignmentsRequest,
    ) -> Self {
        Self { kind, request }
    }

    /// Returns the stable rejection category.
    pub const fn kind(&self) -> AlterPartitionReassignmentsAdmissionErrorKind {
        self.kind
    }

    /// Recovers the exact definitely-unsent request.
    pub fn into_request(self) -> AlterPartitionReassignmentsRequest {
        self.request
    }
}

impl fmt::Display for AlterPartitionReassignmentsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "AlterPartitionReassignments admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AlterPartitionReassignmentsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterPartitionReassignmentsHostError {
    Machine(AlterPartitionReassignmentsMachineError),
    Completion(CompletionRegistryError),
    UnknownOperation,
    MissingSubmission,
    MissingTerminal,
    InvalidHandoff,
    SubmissionMismatch,
    CallCompletion,
    ByteAccounting,
    Unsettled(usize),
    Wake,
}

impl From<AlterPartitionReassignmentsMachineError> for AlterPartitionReassignmentsHostError {
    fn from(error: AlterPartitionReassignmentsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for AlterPartitionReassignmentsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for AlterPartitionReassignmentsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "AlterPartitionReassignments host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for AlterPartitionReassignmentsHostError {}
