//! Named single-observer producer flush over the private engine bridge.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::producer_flush::ProducerFlush as BridgeFlush};

/// Barrier over records accepted before `Producer::flush` was called.
///
/// Dropping a flush abandons observation only. It does not cancel accepted
/// producer work or alter barrier membership.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling an accepted producer flush"]
pub struct Flush {
    inner: BridgeFlush,
}

impl Flush {
    pub(crate) const fn from_bridge(inner: BridgeFlush) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by `Future::poll`.
    pub fn wait(self) -> Result<(), KafkaError> {
        self.inner.wait()
    }
}

impl Future for Flush {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
