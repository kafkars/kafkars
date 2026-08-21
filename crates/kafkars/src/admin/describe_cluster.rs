//! Named single-observer `DescribeCluster` operation for async and blocking Rust.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_describe_operation::AdminDescribeCluster};

use super::ClusterDescription;

/// Sole terminal observer for one submitted `DescribeCluster` call.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeCluster {
    inner: AdminDescribeCluster,
}

impl DescribeCluster {
    pub(crate) const fn from_bridge(inner: AdminDescribeCluster) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<ClusterDescription, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeCluster {
    type Output = Result<ClusterDescription, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
