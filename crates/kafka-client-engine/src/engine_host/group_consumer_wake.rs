//! Explicit join from private group admission to the embedded reactor wake.

use crate::{
    consumer::{GroupConsumerShardWake, GroupConsumerShardWakeError},
    driver::ReactorWake,
};

impl GroupConsumerShardWake for ReactorWake {
    fn request_group_turn(&self) -> Result<(), GroupConsumerShardWakeError> {
        self.request()
            .map_err(|error| GroupConsumerShardWakeError::from_io(error.into_io()))
    }
}
