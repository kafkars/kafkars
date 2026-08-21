//! Named single-observer Admin `DeleteConsumerGroups` operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_delete_consumer_groups::AdminDeleteConsumerGroups};

use super::DeleteConsumerGroupsResult;

/// Sole terminal observer for one submitted Admin `DeleteConsumerGroups` query.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DeleteConsumerGroups {
    inner: AdminDeleteConsumerGroups,
}

impl DeleteConsumerGroups {
    pub(crate) const fn from_bridge(inner: AdminDeleteConsumerGroups) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DeleteConsumerGroupsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DeleteConsumerGroups {
    type Output = Result<DeleteConsumerGroupsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
