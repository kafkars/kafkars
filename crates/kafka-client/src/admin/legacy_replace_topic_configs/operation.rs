//! Named runtime-neutral observation of legacy topic configuration replacement.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::legacy_replace_topic_configs::AdminLegacyReplaceTopicConfigs};

use super::LegacyReplaceTopicConfigsResult;

/// Sole terminal observer for one submitted destructive legacy replacement batch.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct LegacyReplaceTopicConfigs {
    inner: AdminLegacyReplaceTopicConfigs,
}

impl LegacyReplaceTopicConfigs {
    pub(crate) const fn from_bridge(inner: AdminLegacyReplaceTopicConfigs) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<LegacyReplaceTopicConfigsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for LegacyReplaceTopicConfigs {
    type Output = Result<LegacyReplaceTopicConfigsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
