//! Transactional offset admission rejection with exact input recovery.

use core::fmt;

use crate::{Checkpoint, GroupMetadata, KafkaError};

/// Definitely-unsent rejection retaining the exact metadata and checkpoint.
#[must_use = "recover the group metadata and checkpoint before handling the failure"]
pub struct TransactionOffsetsAdmissionError {
    metadata: GroupMetadata,
    checkpoint: Checkpoint,
    error: KafkaError,
}

impl TransactionOffsetsAdmissionError {
    pub(crate) const fn new(
        metadata: GroupMetadata,
        checkpoint: Checkpoint,
        error: KafkaError,
    ) -> Self {
        Self {
            metadata,
            checkpoint,
            error,
        }
    }

    /// Returns the stable semantic admission error.
    pub const fn error(&self) -> &KafkaError {
        &self.error
    }

    /// Borrows the exact rejected group metadata.
    pub const fn metadata(&self) -> &GroupMetadata {
        &self.metadata
    }

    /// Borrows the exact rejected checkpoint.
    pub const fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    /// Recovers both exact inputs and the semantic error.
    pub fn into_parts(self) -> (GroupMetadata, Checkpoint, KafkaError) {
        (self.metadata, self.checkpoint, self.error)
    }
}

impl fmt::Debug for TransactionOffsetsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionOffsetsAdmissionError")
            .field("metadata", &self.metadata)
            .field("checkpoint", &self.checkpoint)
            .field("error", &self.error)
            .finish()
    }
}

impl fmt::Display for TransactionOffsetsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for TransactionOffsetsAdmissionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
