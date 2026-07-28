//! Concrete Admin `CreateAcls` host invariant failures.

use core::fmt;

use kafka_client_core::CreateAclsMachineError;

use crate::admin::create_acls::outcome::CreateAclsTranslationError;
use crate::completion::CompletionRegistryError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreateAclsHostError {
    Machine(CreateAclsMachineError),
    Completion(CompletionRegistryError),
    UnknownOperation,
    MissingSubmission,
    MissingTerminal,
    MissingResultStorage,
    MissingOutcomeStorage,
    SubmissionMismatch,
    InvalidHandoff,
    CallCompletion,
    ByteAccounting,
    Translation(CreateAclsTranslationError),
    Unsettled(usize),
    Wake,
}

impl From<CreateAclsMachineError> for CreateAclsHostError {
    fn from(error: CreateAclsMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for CreateAclsHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for CreateAclsHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin CreateAcls host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for CreateAclsHostError {}
