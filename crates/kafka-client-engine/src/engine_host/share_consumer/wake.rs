//! Explicit join from share admission to the embedded reactor wake.

use crate::{
    consumer::{ShareConsumerShardWake, ShareConsumerShardWakeError},
    driver::ReactorWake,
};

impl ShareConsumerShardWake for ReactorWake {
    fn request_share_turn(&self) -> Result<(), ShareConsumerShardWakeError> {
        self.request()
            .map_err(|error| ShareConsumerShardWakeError::from_io(error.into_io()))
    }
}
