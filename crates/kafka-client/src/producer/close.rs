//! Named single-observer producer close over the private engine bridge.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::producer_barrier::ProducerBarrier as BridgeClose};

/// Atomic first-success producer fence and drain barrier.
///
/// The first accepted close rejects later producer admissions and completes
/// after work accepted before its barrier settles. Dropping this operation
/// abandons observation without reopening or cancelling the producer.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling an accepted producer close"]
pub struct CloseProducer {
    inner: BridgeClose,
}

impl CloseProducer {
    pub(crate) const fn from_bridge(inner: BridgeClose) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by `Future::poll`.
    pub fn wait(self) -> Result<(), KafkaError> {
        self.inner.wait()
    }
}

impl Future for CloseProducer {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
