//! Named runtime-neutral observation of one hosted share-consumer batch.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::share_consumer::ShareConsumerRecv as BridgeRecv};

use super::{ShareConsumer, ShareConsumerBatch};

impl ShareConsumer {
    /// Waits for one already-authorized prefetched share batch.
    ///
    /// Dropping this runtime-neutral operation cancels only observation.
    pub fn recv(&mut self) -> RecvShareConsumerBatch<'_> {
        RecvShareConsumerBatch::from_bridge(self.engine.recv())
    }
}

/// Waits for one already-authorized share-group delivery.
#[derive(Debug)]
#[must_use = "dropping recv abandons only observation; background ShareFetch continues"]
pub struct RecvShareConsumerBatch<'consumer> {
    inner: BridgeRecv<'consumer>,
}

impl<'consumer> RecvShareConsumerBatch<'consumer> {
    const fn from_bridge(inner: BridgeRecv<'consumer>) -> Self {
        Self { inner }
    }

    /// Blocks on the same bounded generation signal used by [`Future::poll`].
    pub fn wait(self) -> Result<Option<ShareConsumerBatch>, KafkaError> {
        self.inner
            .wait()
            .map(|batch| batch.map(ShareConsumerBatch::from_bridge))
    }
}

impl Future for RecvShareConsumerBatch<'_> {
    type Output = Result<Option<ShareConsumerBatch>, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(|batch| batch.map(ShareConsumerBatch::from_bridge)))
    }
}
