//! Named single-observer `CreatePartitions` operation for async and blocking Rust.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_partitions_operation::AdminCreatePartitions};

use super::BatchResult;

/// Sole terminal observer for one submitted `CreatePartitions` batch.
///
/// Dropping abandons observation only and never cancels accepted broker work.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct CreatePartitions {
    inner: AdminCreatePartitions,
}

impl CreatePartitions {
    pub(crate) const fn from_bridge(inner: AdminCreatePartitions) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<BatchResult<String, ()>, KafkaError> {
        self.inner.wait()
    }
}

impl Future for CreatePartitions {
    type Output = Result<BatchResult<String, ()>, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
