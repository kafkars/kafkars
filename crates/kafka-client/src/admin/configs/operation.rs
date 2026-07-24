//! Named single-observer `DescribeConfigs` operation for async and blocking Rust.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_configs_operation::AdminDescribeConfigs};

use super::DescribeConfigsResult;

/// Sole terminal observer for one submitted topic-configuration batch.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeConfigs {
    inner: AdminDescribeConfigs,
}

impl DescribeConfigs {
    pub(crate) const fn from_bridge(inner: AdminDescribeConfigs) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DescribeConfigsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeConfigs {
    type Output = Result<DescribeConfigsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
