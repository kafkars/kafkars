//! Named single-observer `StreamsGroup` offset alteration over the shared owner.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::AlterStreamsGroupOffsetsResult;
use crate::{AlterConsumerGroupOffsets, KafkaError};

/// Sole terminal observer for one submitted `StreamsGroup` offset alteration.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AlterStreamsGroupOffsets {
    inner: AlterConsumerGroupOffsets,
}

impl AlterStreamsGroupOffsets {
    pub(crate) const fn from_consumer_group(inner: AlterConsumerGroupOffsets) -> Self {
        Self { inner }
    }

    /// Blocks on the same consumer-group terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<AlterStreamsGroupOffsetsResult, KafkaError> {
        self.inner
            .wait()
            .map(AlterStreamsGroupOffsetsResult::from_consumer_group)
    }
}

impl Future for AlterStreamsGroupOffsets {
    type Output = Result<AlterStreamsGroupOffsetsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(AlterStreamsGroupOffsetsResult::from_consumer_group))
    }
}
