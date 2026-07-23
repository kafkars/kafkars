//! Driver wake translation for accepted producer-shard work.

use std::io;

use kafka_driver::WakeHandle;

use crate::producer::ingress::{ProducerShardWake, ProducerShardWakeError};

/// Cloneable engine adapter over the embedded driver's coalescing reactor wake.
#[derive(Clone, Debug)]
pub(crate) struct ProducerDriverWake {
    handle: WakeHandle,
}

impl ProducerDriverWake {
    pub(super) const fn new(handle: WakeHandle) -> Self {
        Self { handle }
    }

    /// Requests one coalesced embedded-reactor turn.
    pub(crate) fn request(&self) -> Result<(), ProducerShardWakeError> {
        self.handle.wake().map_err(map_wake_failure)
    }
}

impl ProducerShardWake for ProducerDriverWake {
    fn wake(&self) -> Result<(), ProducerShardWakeError> {
        self.request()
    }
}

pub(super) fn map_wake_failure(source: io::Error) -> ProducerShardWakeError {
    ProducerShardWakeError::from_io(source)
}
