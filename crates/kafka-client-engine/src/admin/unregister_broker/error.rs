//! Immediate admission and host errors for Admin `UnregisterBroker`.

use core::fmt;

use kafka_client_core::UnregisterBrokerMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request rejected before engine ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnregisterBrokerAdmissionErrorKind {
    /// The supplied broker identity is negative.
    InvalidRequest,
    /// The requested duration cannot produce a live absolute deadline.
    InvalidDeadline,
    /// Another bounded host turn currently owns the concrete shard.
    Contended,
    /// Admin admission has closed.
    Closed,
    /// The concrete operation owner has no free slot.
    Capacity,
    /// The request and terminal envelope cannot be reserved.
    RetainedBytes,
    /// Stable operation identities are exhausted.
    IdentityExhausted,
    /// Terminal completion ownership is unavailable.
    HostUnavailable,
}

/// Immediate definitely-unsent Admin `UnregisterBroker` rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnregisterBrokerAdmissionError {
    kind: UnregisterBrokerAdmissionErrorKind,
}

impl UnregisterBrokerAdmissionError {
    pub(crate) const fn new(kind: UnregisterBrokerAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> UnregisterBrokerAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for UnregisterBrokerAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin UnregisterBroker admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for UnregisterBrokerAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnregisterBrokerHostError {
    Machine(UnregisterBrokerMachineError),
    Completion(CompletionRegistryError),
    UnknownOperation,
    MissingSubmission,
    MissingTerminal,
    SubmissionMismatch,
    InvalidHandoff,
    CallCompletion,
    DriverMissing,
    ByteAccounting,
    Unsettled(usize),
    Wake,
}

impl From<UnregisterBrokerMachineError> for UnregisterBrokerHostError {
    fn from(error: UnregisterBrokerMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for UnregisterBrokerHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for UnregisterBrokerHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin UnregisterBroker host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for UnregisterBrokerHostError {}
