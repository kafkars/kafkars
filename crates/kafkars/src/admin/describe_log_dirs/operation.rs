//! Named single-observer broker log-directory description operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_describe_log_dirs::AdminDescribeLogDirs};

use super::DescribeLogDirsResult;

/// Sole terminal observer for one submitted log-directory description.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeLogDirs {
    inner: AdminDescribeLogDirs,
}

impl DescribeLogDirs {
    pub(crate) const fn from_bridge(inner: AdminDescribeLogDirs) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DescribeLogDirsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeLogDirs {
    type Output = Result<DescribeLogDirsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
