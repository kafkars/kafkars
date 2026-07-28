//! Named runtime-neutral observation of one group position replacement.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    KafkaError, bridge::consumer_facade::group_consumer_seek::GroupConsumerSeek as BridgeSeek,
};

use super::{Consumer, StartPosition, TopicPartition};

impl Consumer {
    /// Replaces one assigned partition's next Fetch position.
    ///
    /// This call captures the engine-owned position-resolution deadline before
    /// converting either facade value. It takes no public timeout argument.
    pub fn seek(&mut self, partition: TopicPartition, position: StartPosition) -> Seek<'_> {
        Seek::from_bridge(self.engine.seek(partition, position))
    }
}

/// Waits for one assignment-fenced group position replacement.
#[derive(Debug)]
#[must_use = "dropping seek abandons observation without reversing accepted progress"]
pub struct Seek<'consumer> {
    inner: BridgeSeek<'consumer>,
}

impl<'consumer> Seek<'consumer> {
    pub(crate) const fn from_bridge(inner: BridgeSeek<'consumer>) -> Self {
        Self { inner }
    }

    /// Blocks on the same bounded terminal observation used by [`Future::poll`].
    pub fn wait(self) -> Result<(), KafkaError> {
        self.inner.wait()
    }
}

impl Future for Seek<'_> {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
