//! Local rejection and retained transaction-host invariant failures.

use core::fmt;

use kafka_client_core::TransactionInitializationMachineError;

use crate::completion::CompletionRegistryError;

use super::TransactionInitializationRequest;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionInitializationAdmissionErrorKind {
    InvalidRequest,
    InvalidOperationDeadline,
    Contended,
    Closed,
    Capacity,
    RetainedBytes,
    IdentityExhausted,
    HostUnavailable,
}

#[must_use = "local rejection retains the exact caller input"]
pub(crate) struct TransactionInitializationAdmissionError {
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

    pub(crate) const fn kind(&self) -> TransactionInitializationAdmissionErrorKind {
        self.kind
    }

    pub(crate) fn into_request(self) -> TransactionInitializationRequest {
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
