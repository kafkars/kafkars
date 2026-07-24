//! Inert wake implementation for hosts not attached to the embedded reactor.

use crate::producer::ingress::{ProducerShardWake, ProducerShardWakeError};

pub(crate) struct SilentCompressionWake;

impl ProducerShardWake for SilentCompressionWake {
    fn wake(&self) -> Result<(), ProducerShardWakeError> {
        Ok(())
    }
}
