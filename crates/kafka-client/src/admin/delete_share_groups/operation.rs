//! Named single-observer share-group deletion operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::DeleteShareGroupsResult;
use crate::{KafkaError, admin::DeleteConsumerGroups};

/// Sole terminal observer for one submitted share-group deletion.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DeleteShareGroups {
    inner: DeleteConsumerGroups,
}

impl DeleteShareGroups {
    pub(crate) const fn from_consumer(inner: DeleteConsumerGroups) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DeleteShareGroupsResult, KafkaError> {
        self.inner
            .wait()
            .map(DeleteShareGroupsResult::from_consumer)
    }
}

impl Future for DeleteShareGroups {
    type Output = Result<DeleteShareGroupsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll(context) {
            Poll::Ready(result) => Poll::Ready(result.map(DeleteShareGroupsResult::from_consumer)),
            Poll::Pending => Poll::Pending,
        }
    }
}
