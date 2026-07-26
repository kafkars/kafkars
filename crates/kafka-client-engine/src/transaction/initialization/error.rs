//! Local rejection and retained transaction-host invariant failures.

use core::fmt;

use kafka_client_core::TransactionInitializationMachineError;

use crate::completion::CompletionRegistryError;

use super::TransactionInitializationRequest;

/// Failure to capture the public operation deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionInitializationCaptureError {
    /// The requested duration cannot form one positive absolute deadline.
    InvalidOperationDeadline,
}

impl fmt::Display for TransactionInitializationCaptureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("transaction initialization deadline is invalid")
    }
}

impl std::error::Error for TransactionInitializationCaptureError {}

/// Local rejection before the driver owns transaction initialization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionInitializationAdmissionErrorKind {
    /// The transactional ID or broker timeout is invalid.
    InvalidRequest,
    /// The bounded owner lock is currently contended.
    Contended,
    /// Engine shutdown has closed initialization admission.
    Closed,
    /// The bounded operation or terminal registry is full.
    Capacity,
    /// The bounded transactional-ID envelope is exhausted.
    RetainedBytes,
    /// The engine's nonreused identity space is exhausted.
    IdentityExhausted,
    /// The concrete transaction host is unavailable.
    HostUnavailable,
}

/// Local admission rejection retaining the exact engine request.
#[must_use = "local rejection retains the exact caller input"]
pub struct TransactionInitializationAdmissionError {
    kind: TransactionInitializationAdmissionErrorKind,
    request: TransactionInitializationRequest,
}

impl TransactionInitializationAdmissionError {
    pub(super) const fn new(
        kind: TransactionInitializationAdmissionErrorKind,
        request: TransactionInitializationRequest,
    ) -> Self {
        Self { kind, request }
    }

    /// Returns the stable rejection category.
    pub const fn kind(&self) -> TransactionInitializationAdmissionErrorKind {
        self.kind
    }

    /// Returns the exact request rejected before driver ownership.
    pub fn into_request(self) -> TransactionInitializationRequest {
        self.request
    }
}

impl fmt::Debug for TransactionInitializationAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionInitializationAdmissionError")
            .field("kind", &self.kind)
            .field("request", &self.request)
            .finish()
    }
}

impl fmt::Display for TransactionInitializationAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = self.kind;
        write!(
            formatter,
            "transaction initialization admission failed: {kind:?}"
        )
    }
}

impl std::error::Error for TransactionInitializationAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionInitializationHostError {
    Machine(TransactionInitializationMachineError),
    Completion(CompletionRegistryError),
    UnknownOperation,
    UnexpectedEffect,
    MissingTerminal,
    CallCompletion,
    ByteAccounting,
    OwnerRelease,
    Wake,
    Unsettled(usize),
}

impl From<TransactionInitializationMachineError> for TransactionInitializationHostError {
    fn from(error: TransactionInitializationMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for TransactionInitializationHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for TransactionInitializationHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "transaction initialization host failed: {self:?}"
        )
    }
}

impl std::error::Error for TransactionInitializationHostError {}
