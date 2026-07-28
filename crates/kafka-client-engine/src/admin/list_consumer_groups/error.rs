//! Immediate admission and host-invariant errors for cluster group listing.

use core::fmt;

use kafka_client_core::AdminListConsumerGroupsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListConsumerGroupsAdmissionErrorKind {
    /// The public timeout could not produce a valid absolute deadline.
    InvalidDeadline,
    /// Another thread currently owns the bounded admission shard.
    Contended,
    /// Engine shutdown closed new admissions.
    Closed,
    /// No terminal-completion slot was available.
    Capacity,
    /// The operation could not reserve its bounded retained-byte envelope.
    RetainedBytes,
    /// The completion registry exhausted operation identities.
    IdentityExhausted,
    /// The operation host could not accept work.
    HostUnavailable,
}

/// Immediate definitely-unsent rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListConsumerGroupsAdmissionError {
    kind: ListConsumerGroupsAdmissionErrorKind,
}

impl ListConsumerGroupsAdmissionError {
    pub(crate) const fn new(kind: ListConsumerGroupsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> ListConsumerGroupsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for ListConsumerGroupsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ListConsumerGroups admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for ListConsumerGroupsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListConsumerGroupsHostError {
    Machine(AdminListConsumerGroupsMachineError),
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

impl From<AdminListConsumerGroupsMachineError> for ListConsumerGroupsHostError {
    fn from(error: AdminListConsumerGroupsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for ListConsumerGroupsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for ListConsumerGroupsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ListConsumerGroups host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for ListConsumerGroupsHostError {}
