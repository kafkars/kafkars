//! Exact checkpoint ownership returned by group commit failures.

use core::fmt;

use crate::KafkaError;

use super::Checkpoint;

/// Pre-admission group commit rejection retaining the exact checkpoint.
#[must_use = "commit rejection retains the exact checkpoint for retry or inspection"]
pub struct ConsumerCommitAdmissionError {
    checkpoint: Checkpoint,
    error: KafkaError,
}

/// Accepted commit failure retaining the exact checkpoint when terminal observation permits retry.
#[must_use = "commit failure may retain the exact checkpoint for retry"]
pub struct ConsumerCommitError {
    checkpoint: Option<Checkpoint>,
    error: KafkaError,
}

impl ConsumerCommitError {
    pub(crate) const fn new(checkpoint: Option<Checkpoint>, error: KafkaError) -> Self {
        Self { checkpoint, error }
    }

    /// Borrows the stable semantic terminal error.
    pub const fn error(&self) -> &KafkaError {
        &self.error
    }

    /// Borrows the exact checkpoint retained for retry.
    ///
    /// Observer-lifecycle failures that cannot recover ownership return `None`.
    /// Retained ownership alone is not retry permission: inspect the error's
    /// `RetrySafe` advice before resubmitting this checkpoint. A bounded retry
    /// loop must keep the original deadline rather than renew its timeout.
    pub const fn checkpoint(&self) -> Option<&Checkpoint> {
        self.checkpoint.as_ref()
    }

    /// Returns the exact retry checkpoint, when retained, and semantic error.
    pub fn into_parts(self) -> (Option<Checkpoint>, KafkaError) {
        (self.checkpoint, self.error)
    }
}

impl ConsumerCommitAdmissionError {
    pub(crate) const fn new(checkpoint: Checkpoint, error: KafkaError) -> Self {
        Self { checkpoint, error }
    }

    /// Borrows the stable semantic rejection.
    pub const fn error(&self) -> &KafkaError {
        &self.error
    }

    /// Borrows the exact checkpoint that did not transfer.
    pub const fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    /// Returns the exact checkpoint and stable semantic rejection.
    pub fn into_parts(self) -> (Checkpoint, KafkaError) {
        (self.checkpoint, self.error)
    }
}

impl fmt::Debug for ConsumerCommitAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerCommitAdmissionError")
            .field("checkpoint", &self.checkpoint)
            .field("error", &self.error)
            .finish()
    }
}

impl fmt::Display for ConsumerCommitAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for ConsumerCommitAdmissionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

impl fmt::Debug for ConsumerCommitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerCommitError")
            .field("checkpoint", &self.checkpoint)
            .field("error", &self.error)
            .finish()
    }
}

impl fmt::Display for ConsumerCommitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for ConsumerCommitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
