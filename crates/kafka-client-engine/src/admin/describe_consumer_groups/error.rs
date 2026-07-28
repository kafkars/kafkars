//! Immediate admission and host-invariant errors for group description.

use core::fmt;

use kafka_client_core::AdminDescribeConsumerGroupsMachineError;

use crate::completion::CompletionRegistryError;

/// Stable category for a request that never crossed engine admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeConsumerGroupsAdmissionErrorKind {
    /// The caller supplied an invalid group collection or group ID.
    InvalidRequest,
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
pub struct DescribeConsumerGroupsAdmissionError {
    kind: DescribeConsumerGroupsAdmissionErrorKind,
}

impl DescribeConsumerGroupsAdmissionError {
    pub(crate) const fn new(kind: DescribeConsumerGroupsAdmissionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> DescribeConsumerGroupsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DescribeConsumerGroupsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeConsumerGroups admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for DescribeConsumerGroupsAdmissionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeConsumerGroupsHostError {
    Machine(AdminDescribeConsumerGroupsMachineError),
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

impl From<AdminDescribeConsumerGroupsMachineError> for DescribeConsumerGroupsHostError {
    fn from(error: AdminDescribeConsumerGroupsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DescribeConsumerGroupsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DescribeConsumerGroupsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeConsumerGroups host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for DescribeConsumerGroupsHostError {}
