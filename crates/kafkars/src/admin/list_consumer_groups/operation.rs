//! Named single-observer cluster-wide consumer-group listing operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_list_consumer_groups::AdminListConsumerGroups};

use super::ListConsumerGroupsResult;

/// Sole terminal observer for one submitted cluster-wide listing.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListConsumerGroups {
    inner: AdminListConsumerGroups,
}

impl ListConsumerGroups {
    pub(crate) const fn from_bridge(inner: AdminListConsumerGroups) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<ListConsumerGroupsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for ListConsumerGroups {
    type Output = Result<ListConsumerGroupsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
