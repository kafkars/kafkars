//! Immediate bounded-admission and host errors for Admin `ListOffsets`.

use core::fmt;

use kafka_client_core::AdminListOffsetsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminListOffsetsAdmissionErrorKind {
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

/// Immediate definitely-unsent Admin `ListOffsets` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminListOffsetsAdmissionError {
    kind: AdminListOffsetsAdmissionErrorKind,
}

impl AdminListOffsetsAdmissionError {
    pub(crate) const fn new(kind: AdminListOffsetsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> AdminListOffsetsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for AdminListOffsetsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin ListOffsets admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AdminListOffsetsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdminListOffsetsHostError {
    Machine(AdminListOffsetsMachineError),
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

impl From<AdminListOffsetsMachineError> for AdminListOffsetsHostError {
    fn from(error: AdminListOffsetsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for AdminListOffsetsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for AdminListOffsetsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin ListOffsets host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for AdminListOffsetsHostError {}
