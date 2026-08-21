//! Named single-observer generic `DescribeConfigs` operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_config_resources_operation::AdminDescribeConfigResources};

use super::DescribeConfigResourcesResult;

/// Sole terminal observer for one submitted configuration-resource batch.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeConfigResources {
    inner: AdminDescribeConfigResources,
}

impl DescribeConfigResources {
    pub(crate) const fn from_bridge(inner: AdminDescribeConfigResources) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DescribeConfigResourcesResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeConfigResources {
    type Output = Result<DescribeConfigResourcesResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
