//! Named Streams-group observer over the existing runtime-neutral offset query.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, admin::ListConsumerGroupOffsets};

use super::ListStreamsGroupOffsetsResult;

/// Sole terminal observer for one submitted Streams-group offset query.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListStreamsGroupOffsets {
    inner: ListConsumerGroupOffsets,
}

impl ListStreamsGroupOffsets {
    pub(crate) const fn from_consumer_group(inner: ListConsumerGroupOffsets) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<ListStreamsGroupOffsetsResult, KafkaError> {
        self.inner
            .wait()
            .map(ListStreamsGroupOffsetsResult::from_consumer_group)
    }
}

impl Future for ListStreamsGroupOffsets {
    type Output = Result<ListStreamsGroupOffsetsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                Poll::Ready(result.map(ListStreamsGroupOffsetsResult::from_consumer_group))
            }
        }
    }
}
