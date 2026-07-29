//! Named observer for destructive generic legacy configuration replacement.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::legacy_replace_topic_configs::AdminLegacyReplaceConfigResources};

use super::LegacyReplaceConfigResourcesResult;

/// Sole terminal observer for one submitted generic replacement batch.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct LegacyReplaceConfigResources {
    inner: AdminLegacyReplaceConfigResources,
}

impl LegacyReplaceConfigResources {
    pub(crate) const fn from_bridge(inner: AdminLegacyReplaceConfigResources) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<LegacyReplaceConfigResourcesResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for LegacyReplaceConfigResources {
    type Output = Result<LegacyReplaceConfigResourcesResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
