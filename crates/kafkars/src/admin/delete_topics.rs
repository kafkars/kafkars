//! Named single-observer `DeleteTopics` operation for async and blocking Rust.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_delete_operation::AdminDeleteTopics};

use super::BatchResult;

/// Sole terminal observer for one submitted `DeleteTopics` batch.
///
/// Dropping abandons observation only. It does not cancel destructive work
/// that may already have reached Kafka.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DeleteTopics {
    inner: AdminDeleteTopics,
}

impl DeleteTopics {
    pub(crate) const fn from_bridge(inner: AdminDeleteTopics) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<BatchResult<String, ()>, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DeleteTopics {
    type Output = Result<BatchResult<String, ()>, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
