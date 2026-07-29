//! Inert single-transaction termination intent over the producer-fencing owner.

use std::time::Duration;

use super::ForceTerminateTransaction;
use crate::admin::FenceProducersBuilder;

/// Inert request to force-terminate one transaction by transactional ID.
#[derive(Debug)]
#[must_use = "call submit to admit the ForceTerminateTransaction operation"]
pub struct ForceTerminateTransactionBuilder {
    inner: FenceProducersBuilder,
}

impl ForceTerminateTransactionBuilder {
    pub(crate) const fn from_fence_producers(inner: FenceProducersBuilder) -> Self {
        Self { inner }
    }

    /// Replaces the timeout while retaining the original Admin call boundary.
    pub fn deadline_after(mut self, timeout: Duration) -> Self {
        self.inner = self.inner.deadline_after(timeout);
        self
    }

    /// Attempts bounded admission and returns one named terminal observer.
    pub fn submit(self) -> ForceTerminateTransaction {
        ForceTerminateTransaction::from_fence_producers(self.inner.submit())
    }
}
