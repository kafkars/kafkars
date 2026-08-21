//! Named single-observer streams-group deletion operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::DeleteStreamsGroupsResult;
use crate::{KafkaError, admin::DeleteConsumerGroups};

/// Sole terminal observer for one submitted streams-group deletion.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DeleteStreamsGroups {
    inner: DeleteConsumerGroups,
}

impl DeleteStreamsGroups {
    pub(crate) const fn from_consumer(inner: DeleteConsumerGroups) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DeleteStreamsGroupsResult, KafkaError> {
        self.inner
            .wait()
            .map(DeleteStreamsGroupsResult::from_consumer)
    }
}

impl Future for DeleteStreamsGroups {
    type Output = Result<DeleteStreamsGroupsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll(context) {
            Poll::Ready(result) => {
                Poll::Ready(result.map(DeleteStreamsGroupsResult::from_consumer))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
