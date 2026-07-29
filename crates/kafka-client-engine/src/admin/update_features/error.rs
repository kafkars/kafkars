//! Immediate admission and invariant errors for Admin `UpdateFeatures`.

use core::fmt;

use kafka_client_core::UpdateFeaturesMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request rejected before engine ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateFeaturesAdmissionErrorKind {
    /// The engine-owned request is not a valid bounded update plan.
    InvalidRequest,
    /// The requested duration cannot produce a live absolute deadline.
    InvalidDeadline,
    /// Another bounded host turn currently owns the concrete shard.
    Contended,
    /// Admin admission has closed.
    Closed,
    /// The concrete operation owner has no free slot.
    Capacity,
    /// The complete request and result envelope cannot be reserved.
    RetainedBytes,
    /// Stable operation identities are exhausted.
    IdentityExhausted,
    /// Terminal completion ownership is unavailable.
    HostUnavailable,
}

/// Immediate definitely-unsent Admin `UpdateFeatures` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UpdateFeaturesAdmissionError {
    kind: UpdateFeaturesAdmissionErrorKind,
}

impl UpdateFeaturesAdmissionError {
    pub(crate) const fn new(kind: UpdateFeaturesAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> UpdateFeaturesAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for UpdateFeaturesAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin UpdateFeatures admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for UpdateFeaturesAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UpdateFeaturesHostError {
    Machine(UpdateFeaturesMachineError),
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

impl From<UpdateFeaturesMachineError> for UpdateFeaturesHostError {
    fn from(error: UpdateFeaturesMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for UpdateFeaturesHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for UpdateFeaturesHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin UpdateFeatures host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for UpdateFeaturesHostError {}
