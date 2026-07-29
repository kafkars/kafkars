//! Admission and retained host failures for leader election.

use core::fmt;

use kafka_client_core::ElectLeadersMachineError;

use crate::completion::CompletionRegistryError;

use super::ElectLeadersRequest;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ElectLeadersAdmissionErrorKind {
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

/// Immediate definitely-unsent election rejection.
#[derive(Debug)]
pub struct ElectLeadersAdmissionError {
    kind: ElectLeadersAdmissionErrorKind,
    request: ElectLeadersRequest,
}

impl ElectLeadersAdmissionError {
    pub(crate) const fn new(
        kind: ElectLeadersAdmissionErrorKind,
        request: ElectLeadersRequest,
    ) -> Self {
        Self { kind, request }
    }

    /// Returns the stable rejection category.
    pub const fn kind(&self) -> ElectLeadersAdmissionErrorKind {
        self.kind
    }

    /// Recovers the exact definitely-unsent request.
    pub fn into_request(self) -> ElectLeadersRequest {
        self.request
    }
}

impl fmt::Display for ElectLeadersAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ElectLeaders admission failed: {:?}", self.kind)
    }
}

impl std::error::Error for ElectLeadersAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ElectLeadersHostError {
    Machine(ElectLeadersMachineError),
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

impl From<ElectLeadersMachineError> for ElectLeadersHostError {
    fn from(error: ElectLeadersMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for ElectLeadersHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for ElectLeadersHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ElectLeaders host invariant failed: {self:?}")
    }
}

impl std::error::Error for ElectLeadersHostError {}
