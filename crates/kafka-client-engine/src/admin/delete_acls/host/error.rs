//! Concrete Admin `DeleteAcls` host invariant failures.

use core::fmt;

use kafka_client_core::DeleteAclsMachineError;

use crate::completion::CompletionRegistryError;

use super::super::DeleteAclsTranslationError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteAclsHostError {
    Machine(DeleteAclsMachineError),
    Completion(CompletionRegistryError),
    Translation(DeleteAclsTranslationError),
    UnknownOperation,
    MissingSubmission,
    MissingTerminal,
    MissingResultStorage,
    MissingOutcomeStorage,
    SubmissionMismatch,
    InvalidHandoff,
    CallCompletion,
    ByteAccounting,
    Unsettled(usize),
    Wake,
}

impl From<DeleteAclsMachineError> for DeleteAclsHostError {
    fn from(error: DeleteAclsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for DeleteAclsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for DeleteAclsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DeleteAcls host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for DeleteAclsHostError {}
