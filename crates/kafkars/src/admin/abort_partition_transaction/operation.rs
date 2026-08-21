//! Named single-observer partition-transaction abort operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::abort_partition_transaction::AdminAbortPartitionTransaction};

/// Sole terminal observer for one submitted partition-transaction abort.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AbortPartitionTransaction {
    inner: AdminAbortPartitionTransaction,
}

impl AbortPartitionTransaction {
    pub(crate) const fn from_bridge(inner: AdminAbortPartitionTransaction) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<(), KafkaError> {
        self.inner.wait()
    }
}

impl Future for AbortPartitionTransaction {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
