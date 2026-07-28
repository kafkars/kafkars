//! Admission and retained host failures for static-member removal.

use core::fmt;

use kafka_client_core::RemoveConsumerGroupMembersMachineError;

use crate::completion::CompletionRegistryError;

use super::RemoveConsumerGroupMembersRequest;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveConsumerGroupMembersAdmissionErrorKind {
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

/// Immediate definitely-unsent member-removal rejection.
#[derive(Debug)]
pub struct RemoveConsumerGroupMembersAdmissionError {
    kind: RemoveConsumerGroupMembersAdmissionErrorKind,
    request: RemoveConsumerGroupMembersRequest,
}

impl RemoveConsumerGroupMembersAdmissionError {
    pub(crate) const fn new(
        kind: RemoveConsumerGroupMembersAdmissionErrorKind,
        request: RemoveConsumerGroupMembersRequest,
    ) -> Self {
        Self { kind, request }
    }

    /// Returns the stable rejection category.
    pub const fn kind(&self) -> RemoveConsumerGroupMembersAdmissionErrorKind {
        self.kind
    }

    /// Recovers the exact definitely-unsent request.
    pub fn into_request(self) -> RemoveConsumerGroupMembersRequest {
        self.request
    }
}

impl fmt::Display for RemoveConsumerGroupMembersAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "RemoveConsumerGroupMembers admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for RemoveConsumerGroupMembersAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RemoveConsumerGroupMembersHostError {
    Machine(RemoveConsumerGroupMembersMachineError),
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

impl From<RemoveConsumerGroupMembersMachineError> for RemoveConsumerGroupMembersHostError {
    fn from(error: RemoveConsumerGroupMembersMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for RemoveConsumerGroupMembersHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for RemoveConsumerGroupMembersHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "RemoveConsumerGroupMembers host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for RemoveConsumerGroupMembersHostError {}
