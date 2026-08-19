//! Named single-observer `ShareGroup` description operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::describe_share_group::AdminDescribeShareGroup};

use super::DescribeShareGroupResult;

/// Sole terminal observer for one submitted `ShareGroup` description.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeShareGroup {
    inner: AdminDescribeShareGroup,
}

impl DescribeShareGroup {
    pub(crate) const fn from_bridge(inner: AdminDescribeShareGroup) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DescribeShareGroupResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeShareGroup {
    type Output = Result<DescribeShareGroupResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
