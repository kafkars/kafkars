//! Explicit join from assigned-consumer admission to the embedded reactor wake.

use crate::{
    consumer::{AssignedConsumerShardWake, AssignedConsumerShardWakeError},
    driver::{ReactorWake, ReactorWakeError},
};

impl AssignedConsumerShardWake for ReactorWake {
    fn request_assigned_turn(&self) -> Result<(), AssignedConsumerShardWakeError> {
        self.request().map_err(AssignedConsumerShardWakeError::from)
    }
}

impl From<ReactorWakeError> for AssignedConsumerShardWakeError {
    fn from(error: ReactorWakeError) -> Self {
        Self::from_io(error.into_io())
    }
}
