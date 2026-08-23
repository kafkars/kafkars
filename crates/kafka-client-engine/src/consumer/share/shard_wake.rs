//! Share-consumer adaptation boundary for one coalesced host-turn request.

use std::{fmt, io};

pub(crate) trait ShareConsumerShardWake: Send + Sync + 'static {
    fn request_share_turn(&self) -> Result<(), ShareConsumerShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct ShareConsumerShardWakeError {
    source: io::Error,
}

impl ShareConsumerShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for ShareConsumerShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "share-consumer shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for ShareConsumerShardWakeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
