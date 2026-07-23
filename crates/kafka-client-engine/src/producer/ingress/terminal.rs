//! Typed shard-wide terminal cleanup failures.

use std::{error::Error, fmt};

use crate::{
    completion::CompletionRegistryError, producer::shutdown::ProducerTerminalCleanupError,
};

/// Shard-wide terminal cleanup failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerShardTerminalError {
    Host(ProducerTerminalCleanupError),
    Completion(CompletionRegistryError),
}

impl From<ProducerTerminalCleanupError> for ProducerShardTerminalError {
    fn from(error: ProducerTerminalCleanupError) -> Self {
        Self::Host(error)
    }
}

impl From<CompletionRegistryError> for ProducerShardTerminalError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl fmt::Display for ProducerShardTerminalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Host(error) => error.fmt(formatter),
            Self::Completion(error) => error.fmt(formatter),
        }
    }
}

impl Error for ProducerShardTerminalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Host(error) => Some(error),
            Self::Completion(error) => Some(error),
        }
    }
}
