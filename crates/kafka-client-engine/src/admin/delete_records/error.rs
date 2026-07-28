//! Immediate bounded-admission and host errors for Admin `DeleteRecords`.

use core::fmt;

use kafka_client_core::DeleteRecordsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteRecordsAdmissionErrorKind {
    /// One topic, partition, specification, or batch shape is invalid.
    InvalidRequest,
    /// The requested duration cannot become one absolute deadline.
    InvalidDeadline,
    /// Another bounded host turn briefly owns the concrete shard.
    Contended,
    /// Admin admission has closed.
    Closed,
    /// The concrete operation owner has no free slot.
    Capacity,
    /// The complete request and terminal envelope cannot be reserved.
    RetainedBytes,
    /// Stable operation identities are exhausted.
    IdentityExhausted,
    /// Terminal completion ownership is unavailable.
    HostUnavailable,
}

/// Immediate definitely-unsent Admin `DeleteRecords` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteRecordsAdmissionError {
    kind: DeleteRecordsAdmissionErrorKind,
}

impl DeleteRecordsAdmissionError {
    pub(crate) const fn new(kind: DeleteRecordsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DeleteRecordsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DeleteRecordsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DeleteRecords admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for DeleteRecordsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteRecordsHostError {
    Machine(DeleteRecordsMachineError),
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

impl From<DeleteRecordsMachineError> for DeleteRecordsHostError {
    fn from(error: DeleteRecordsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DeleteRecordsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DeleteRecordsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DeleteRecords host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for DeleteRecordsHostError {}
