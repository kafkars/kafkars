//! Named single-observer client-metrics resource listing operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::list_client_metrics_resources::AdminListClientMetricsResources};

use super::ListClientMetricsResourcesResult;

/// Sole terminal observer for one submitted client-metrics resource listing.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListClientMetricsResources {
    inner: AdminListClientMetricsResources,
}

impl ListClientMetricsResources {
    pub(crate) const fn from_bridge(inner: AdminListClientMetricsResources) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<ListClientMetricsResourcesResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for ListClientMetricsResources {
    type Output = Result<ListClientMetricsResourcesResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
