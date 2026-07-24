//! Assigned-consumer adaptation over the domain-neutral reactor wake.

use std::{fmt, io};

pub(crate) trait AssignedConsumerShardWake: Send + Sync + 'static {
    fn request_assigned_turn(&self) -> Result<(), AssignedConsumerShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct AssignedConsumerShardWakeError {
    source: io::Error,
}

impl AssignedConsumerShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for AssignedConsumerShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "assigned-consumer shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for AssignedConsumerShardWakeError {}
