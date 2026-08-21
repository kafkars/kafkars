//! Named runtime-neutral observation of one hosted group-consumer batch.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    KafkaError, bridge::consumer_facade::group_consumer_recv::GroupConsumerRecv as BridgeRecv,
};

use super::{Consumer, ConsumerBatch};

impl Consumer {
    /// Takes one already-authorized prefetched group batch when available.
    ///
    /// This starts no Fetch or membership work and has no application timeout.
    pub fn try_take_batch(&mut self) -> Result<Option<ConsumerBatch>, KafkaError> {
        self.engine
            .try_take_batch()
            .map(|batch| batch.map(ConsumerBatch::from_bridge))
    }

    /// Waits for one already-authorized prefetched group batch.
    ///
    /// Dropping this runtime-neutral operation cancels only observation.
    pub fn recv(&mut self) -> RecvConsumerBatch<'_> {
        RecvConsumerBatch::from_bridge(self.engine.recv())
    }
}

/// Waits for one already-authorized classic-group delivery.
#[derive(Debug)]
#[must_use = "dropping recv abandons only observation; background Fetch continues"]
pub struct RecvConsumerBatch<'consumer> {
    inner: BridgeRecv<'consumer>,
}

impl<'consumer> RecvConsumerBatch<'consumer> {
    pub(crate) const fn from_bridge(inner: BridgeRecv<'consumer>) -> Self {
        Self { inner }
    }

    /// Blocks on the same bounded generation signal used by [`Future::poll`].
    pub fn wait(self) -> Result<Option<ConsumerBatch>, KafkaError> {
        self.inner
            .wait()
            .map(|batch| batch.map(ConsumerBatch::from_bridge))
    }
}

impl Future for RecvConsumerBatch<'_> {
    type Output = Result<Option<ConsumerBatch>, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(|batch| batch.map(ConsumerBatch::from_bridge)))
    }
}
