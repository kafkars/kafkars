//! Named single-observer `IncrementalAlterConfigs` operation for async and blocking Rust.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_alter_configs_operation::AdminIncrementalAlterConfigs};

use super::IncrementalAlterConfigsResult;

/// Sole terminal observer for one submitted topic configuration alteration batch.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct IncrementalAlterConfigs {
    inner: AdminIncrementalAlterConfigs,
}

impl IncrementalAlterConfigs {
    pub(crate) const fn from_bridge(inner: AdminIncrementalAlterConfigs) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<IncrementalAlterConfigsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for IncrementalAlterConfigs {
    type Output = Result<IncrementalAlterConfigsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
