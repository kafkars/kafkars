//! Named Streams-group observer over one consumer-group offset deletion.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::DeleteStreamsGroupOffsetsResult;
use crate::{DeleteConsumerGroupOffsets, KafkaError};

/// Sole terminal observer for one submitted Streams-group offset deletion.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DeleteStreamsGroupOffsets {
    inner: DeleteConsumerGroupOffsets,
}

impl DeleteStreamsGroupOffsets {
    pub(crate) const fn from_consumer(inner: DeleteConsumerGroupOffsets) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DeleteStreamsGroupOffsetsResult, KafkaError> {
        self.inner
            .wait()
            .map(DeleteStreamsGroupOffsetsResult::from_consumer)
    }
}

impl Future for DeleteStreamsGroupOffsets {
    type Output = Result<DeleteStreamsGroupOffsetsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll(context) {
            Poll::Ready(Ok(result)) => {
                Poll::Ready(Ok(DeleteStreamsGroupOffsetsResult::from_consumer(result)))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}
