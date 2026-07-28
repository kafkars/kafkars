//! Synchronous assignment-fenced processing acknowledgment.

use super::{Checkpoint, Consumer, ConsumerAcknowledgeError};

impl Consumer {
    /// Renews application-processing liveness for one current checkpoint.
    ///
    /// This consumes the assignment-fenced checkpoint without committing its
    /// offset to Kafka or starting a public operation timeout. Rejection
    /// returns the exact checkpoint for inspection or retry.
    #[expect(
        clippy::result_large_err,
        reason = "rejection returns the exact assignment-fenced checkpoint"
    )]
    pub fn acknowledge(&mut self, checkpoint: Checkpoint) -> Result<(), ConsumerAcknowledgeError> {
        self.engine
            .acknowledge(checkpoint.into_bridge())
            .map_err(|(checkpoint, error)| {
                ConsumerAcknowledgeError::new(Checkpoint::from_bridge(checkpoint), error)
            })
    }
}
