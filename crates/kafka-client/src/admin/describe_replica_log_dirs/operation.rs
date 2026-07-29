//! Named single-observer selected-replica log-directory description operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_describe_replica_log_dirs::AdminDescribeReplicaLogDirs};

use super::DescribeReplicaLogDirsResult;

/// Sole terminal observer for one selected-replica description.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeReplicaLogDirs {
    inner: AdminDescribeReplicaLogDirs,
}

impl DescribeReplicaLogDirs {
    pub(crate) const fn from_bridge(inner: AdminDescribeReplicaLogDirs) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DescribeReplicaLogDirsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeReplicaLogDirs {
    type Output = Result<DescribeReplicaLogDirsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
