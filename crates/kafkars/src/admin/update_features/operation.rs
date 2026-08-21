//! Named single-observer finalized-feature update operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::update_features::AdminUpdateFeatures};

use super::UpdateFeaturesResult;

/// Sole terminal observer for one submitted finalized-feature update.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct UpdateFeatures {
    inner: AdminUpdateFeatures,
}

impl UpdateFeatures {
    pub(crate) const fn from_bridge(inner: AdminUpdateFeatures) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<UpdateFeaturesResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for UpdateFeatures {
    type Output = Result<UpdateFeaturesResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
