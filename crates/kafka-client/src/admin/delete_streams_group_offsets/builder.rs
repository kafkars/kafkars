//! Inert Streams-group offset-deletion intent over the existing bounded owner.

use std::time::Duration;

use super::DeleteStreamsGroupOffsets;
use crate::DeleteConsumerGroupOffsetsBuilder;

/// Inert caller-ordered committed-offset deletion for one Streams group.
#[derive(Debug)]
#[must_use = "call submit to admit the DeleteStreamsGroupOffsets operation"]
pub struct DeleteStreamsGroupOffsetsBuilder {
    inner: DeleteConsumerGroupOffsetsBuilder,
}

impl DeleteStreamsGroupOffsetsBuilder {
    pub(crate) const fn from_consumer(inner: DeleteConsumerGroupOffsetsBuilder) -> Self {
        Self { inner }
    }

    /// Replaces the duration converted into an absolute deadline at submission.
    pub fn deadline_after(self, timeout: Duration) -> Self {
        Self {
            inner: self.inner.deadline_after(timeout),
        }
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> DeleteStreamsGroupOffsets {
        DeleteStreamsGroupOffsets::from_consumer(self.inner.submit())
    }
}
