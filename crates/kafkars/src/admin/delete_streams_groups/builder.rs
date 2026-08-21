//! Inert streams-group deletion intent with one submission boundary.

use std::time::Duration;

use super::DeleteStreamsGroups;
use crate::admin::DeleteConsumerGroupsBuilder;

/// Inert caller-ordered deletion for streams groups.
#[must_use = "call submit to admit the DeleteStreamsGroups operation"]
pub struct DeleteStreamsGroupsBuilder {
    inner: DeleteConsumerGroupsBuilder,
}

impl DeleteStreamsGroupsBuilder {
    pub(crate) const fn from_consumer(inner: DeleteConsumerGroupsBuilder) -> Self {
        Self { inner }
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub fn deadline_after(self, timeout: Duration) -> Self {
        Self {
            inner: self.inner.deadline_after(timeout),
        }
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> DeleteStreamsGroups {
        DeleteStreamsGroups::from_consumer(self.inner.submit())
    }
}

impl std::fmt::Debug for DeleteStreamsGroupsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeleteStreamsGroupsBuilder")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}
