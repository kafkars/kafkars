//! Producer-shard adaptation over the shared domain-neutral reactor wake.

use crate::driver::{ReactorWake, ReactorWakeError};

use super::{ProducerShardWake, ProducerShardWakeError};

impl ProducerShardWake for ReactorWake {
    fn wake(&self) -> Result<(), ProducerShardWakeError> {
        self.request().map_err(ProducerShardWakeError::from)
    }
}

impl From<ReactorWakeError> for ProducerShardWakeError {
    fn from(error: ReactorWakeError) -> Self {
        Self::from_io(error.into_io())
    }
}
