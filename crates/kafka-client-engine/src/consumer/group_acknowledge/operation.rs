//! Immediate state-transition entry point on one unique group handle.

use crate::consumer::{
    GroupConsumerCheckpoint, GroupConsumerHandle, group_acknowledge::GroupConsumerAcknowledgeError,
};

impl GroupConsumerHandle {
    /// Renews processing liveness for the exact assignment owning `checkpoint`.
    ///
    /// Time is captured before shard contention. This immediate state
    /// transition starts no protocol work and reserves no completion or bytes.
    pub fn acknowledge(
        &mut self,
        checkpoint: GroupConsumerCheckpoint,
    ) -> Result<(), GroupConsumerAcknowledgeError> {
        let fence = checkpoint.position_fence();
        self.port
            .try_acknowledge_processing(self.group_id, fence)
            .map_err(|error| GroupConsumerAcknowledgeError::from_port(error, checkpoint))
    }
}
