//! Group-consumer adaptation boundary for one coalesced host-turn request.

use std::{fmt, io};

pub(crate) trait GroupConsumerShardWake: Send + Sync + 'static {
    fn request_group_turn(&self) -> Result<(), GroupConsumerShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct GroupConsumerShardWakeError {
    source: io::Error,
}

impl GroupConsumerShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for GroupConsumerShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "group-consumer shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for GroupConsumerShardWakeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
