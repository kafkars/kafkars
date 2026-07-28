//! Named single-observer client readiness operation for async and blocking Rust.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_describe_operation::AdminDescribeCluster};

/// Sole observer for one point-in-time client readiness probe.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted readiness work"]
pub struct Ready {
    inner: AdminDescribeCluster,
}

impl Ready {
    pub(crate) const fn from_bridge(inner: AdminDescribeCluster) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<(), KafkaError> {
        self.inner.wait().map(drop)
    }
}

impl Future for Ready {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner)
            .poll(context)
            .map(|result| result.map(drop))
    }
}
