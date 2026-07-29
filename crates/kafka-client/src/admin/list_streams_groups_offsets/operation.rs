//! Typed observer over one multi-consumer-group OffsetFetch operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, admin::ListConsumerGroupsOffsets};

use super::ListStreamsGroupsOffsetsResult;

/// Sole observer for one accepted multi-Streams-group offset query.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListStreamsGroupsOffsets {
    inner: ListConsumerGroupsOffsets,
}

impl ListStreamsGroupsOffsets {
    pub(crate) const fn from_consumer_groups(inner: ListConsumerGroupsOffsets) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<ListStreamsGroupsOffsetsResult, KafkaError> {
        self.inner
            .wait()
            .map(ListStreamsGroupsOffsetsResult::from_consumer_groups)
    }
}

impl Future for ListStreamsGroupsOffsets {
    type Output = Result<ListStreamsGroupsOffsetsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll(context) {
            Poll::Ready(result) => {
                Poll::Ready(result.map(ListStreamsGroupsOffsetsResult::from_consumer_groups))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
