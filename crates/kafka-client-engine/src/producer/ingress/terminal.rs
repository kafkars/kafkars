//! Typed refusal when pending ownership blocks shard-wide terminal cleanup.

use std::{error::Error, fmt};

use crate::{
    completion::CompletionRegistryError,
    producer::{pending::PendingPrimaryMissingError, shutdown::ProducerTerminalCleanupError},
};

/// Exact pre-core ownership that must settle before shard cleanup can proceed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerShardPendingOwnership {
    pub(crate) records: usize,
    pub(crate) retained_bytes: usize,
    pub(crate) notification_permits: usize,
}

impl ProducerShardPendingOwnership {
    pub(super) const fn new(
        records: usize,
        retained_bytes: usize,
        notification_permits: usize,
    ) -> Self {
        Self {
            records,
            retained_bytes,
            notification_permits,
        }
    }

    pub(super) const fn is_empty(self) -> bool {
        self.records == 0 && self.retained_bytes == 0 && self.notification_permits == 0
    }
}

/// Shard-wide refusal preserving the distinction between pending and host ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerShardTerminalError {
    Pending(ProducerShardPendingOwnership),
    Host(ProducerTerminalCleanupError),
    Completion(CompletionRegistryError),
    PendingPrimaryMissing(PendingPrimaryMissingError),
}

impl ProducerShardTerminalError {
    pub(crate) const fn pending_ownership(self) -> Option<ProducerShardPendingOwnership> {
        match self {
            Self::Pending(ownership) => Some(ownership),
            Self::Host(_) | Self::Completion(_) | Self::PendingPrimaryMissing(_) => None,
        }
    }
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

impl From<PendingPrimaryMissingError> for ProducerShardTerminalError {
    fn from(error: PendingPrimaryMissingError) -> Self {
        Self::PendingPrimaryMissing(error)
    }
}

impl fmt::Display for ProducerShardTerminalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending(ownership) => write!(
                formatter,
                "pending producer ownership remains: records={}, retained_bytes={}, \
                 notification_permits={}",
                ownership.records, ownership.retained_bytes, ownership.notification_permits
            ),
            Self::Host(error) => error.fmt(formatter),
            Self::Completion(error) => error.fmt(formatter),
            Self::PendingPrimaryMissing(error) => error.fmt(formatter),
        }
    }
}

impl Error for ProducerShardTerminalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Host(error) => Some(error),
            Self::Completion(error) => Some(error),
            Self::PendingPrimaryMissing(error) => Some(error),
            Self::Pending(_) => None,
        }
    }
}
