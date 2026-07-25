//! Stable local failure vocabulary for group offset-commit host invariants.

use core::fmt;

use kafka_client_core::GroupOffsetCommitMachineError;

use crate::completion::CompletionRegistryError;

use super::super::session_catalog::GroupSessionCatalogError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupOffsetCommitHostError {
    Machine(GroupOffsetCommitMachineError),
    Completion(CompletionRegistryError),
    Catalog(GroupSessionCatalogError),
    UnknownOperation,
    MissingPrepared,
    MissingTerminal,
    UnexpectedEffect,
    Preparation,
    ByteAccounting,
    Settlement,
    DriverCompletion,
    Unsettled,
}

impl From<GroupOffsetCommitMachineError> for GroupOffsetCommitHostError {
    fn from(error: GroupOffsetCommitMachineError) -> Self {
        Self::Machine(error)
    }
}

impl From<CompletionRegistryError> for GroupOffsetCommitHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl From<GroupSessionCatalogError> for GroupOffsetCommitHostError {
    fn from(error: GroupSessionCatalogError) -> Self {
        Self::Catalog(error)
    }
}

impl fmt::Display for GroupOffsetCommitHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "group offset commit host invariant failed: {self:?}"
        )
    }
}

impl std::error::Error for GroupOffsetCommitHostError {}
