//! Exact public checkpoint ownership returned by acknowledgment rejection.

use core::fmt;

use crate::KafkaError;

use super::Checkpoint;

/// Rejected processing acknowledgment retaining the exact checkpoint.
#[must_use = "acknowledgment rejection retains the exact checkpoint for retry"]
pub struct ConsumerAcknowledgeError {
    checkpoint: Checkpoint,
    error: KafkaError,
}

impl ConsumerAcknowledgeError {
    pub(crate) const fn new(checkpoint: Checkpoint, error: KafkaError) -> Self {
        Self { checkpoint, error }
    }

    /// Borrows the stable semantic rejection.
    pub const fn error(&self) -> &KafkaError {
        &self.error
    }

    /// Borrows the exact checkpoint whose progress was not accepted.
    pub const fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    /// Returns the exact rejected checkpoint and its stable semantic error.
    pub fn into_parts(self) -> (Checkpoint, KafkaError) {
        (self.checkpoint, self.error)
    }
}

impl fmt::Debug for ConsumerAcknowledgeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerAcknowledgeError")
            .field("checkpoint", &self.checkpoint)
            .field("error", &self.error)
            .finish()
    }
}

impl fmt::Display for ConsumerAcknowledgeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for ConsumerAcknowledgeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
