//! Inert share-group deletion intent with one submission boundary.

use std::time::Duration;

use super::DeleteShareGroups;
use crate::admin::DeleteConsumerGroupsBuilder;

/// Inert caller-ordered deletion for share groups.
#[must_use = "call submit to admit the DeleteShareGroups operation"]
pub struct DeleteShareGroupsBuilder {
    inner: DeleteConsumerGroupsBuilder,
}

impl DeleteShareGroupsBuilder {
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
    pub fn submit(self) -> DeleteShareGroups {
        DeleteShareGroups::from_consumer(self.inner.submit())
    }
}

impl std::fmt::Debug for DeleteShareGroupsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeleteShareGroupsBuilder")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}
