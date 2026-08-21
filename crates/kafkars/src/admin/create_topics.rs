//! Named single-observer `CreateTopics` operation for async and blocking Rust.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_operation::AdminCreateTopics};

use super::BatchResult;

/// Sole terminal observer for one submitted `CreateTopics` batch.
///
/// Dropping this value abandons observation only. It does not cancel accepted
/// work. This first admin slice exposes no cancellation command; client
/// shutdown closes admission and settles accepted operations.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct CreateTopics {
    inner: AdminCreateTopics,
}

impl CreateTopics {
    pub(crate) const fn from_bridge(inner: AdminCreateTopics) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<BatchResult<String, ()>, KafkaError> {
        self.inner.wait()
    }
}

impl Future for CreateTopics {
    type Output = Result<BatchResult<String, ()>, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
