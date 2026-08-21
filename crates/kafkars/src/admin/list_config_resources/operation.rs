//! Named single-observer configuration-resource listing operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::list_config_resources::AdminListConfigResources};

use super::ListConfigResourcesResult;

/// Sole terminal observer for one submitted configuration-resource listing.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListConfigResources {
    inner: AdminListConfigResources,
}

impl ListConfigResources {
    pub(crate) const fn from_bridge(inner: AdminListConfigResources) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<ListConfigResourcesResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for ListConfigResources {
    type Output = Result<ListConfigResourcesResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
