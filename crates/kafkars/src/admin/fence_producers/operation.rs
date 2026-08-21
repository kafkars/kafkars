//! Named single-observer producer-fencing operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::fence_producers::AdminFenceProducers};

use super::FenceProducersResult;

/// Sole runtime-neutral observer for one submitted producer-fencing batch.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct FenceProducers {
    inner: AdminFenceProducers,
}

impl FenceProducers {
    pub(crate) const fn from_bridge(inner: AdminFenceProducers) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal result observed by [`Future::poll`].
    pub fn wait(self) -> Result<FenceProducersResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for FenceProducers {
    type Output = Result<FenceProducersResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
