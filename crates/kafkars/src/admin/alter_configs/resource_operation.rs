//! Named single-observer generic `IncrementalAlterConfigs` operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    KafkaError,
    bridge::admin_alter_config_resources_operation::AdminIncrementalAlterConfigResources,
};

use super::IncrementalAlterConfigResourcesResult;

/// Sole observer for one submitted configuration-resource alteration batch.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct IncrementalAlterConfigResources {
    inner: AdminIncrementalAlterConfigResources,
}

impl IncrementalAlterConfigResources {
    pub(crate) const fn from_bridge(inner: AdminIncrementalAlterConfigResources) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<IncrementalAlterConfigResourcesResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for IncrementalAlterConfigResources {
    type Output = Result<IncrementalAlterConfigResourcesResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
