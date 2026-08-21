//! Named single-observer classic-group description operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_describe_classic_groups::AdminDescribeClassicGroups};

use super::DescribeClassicGroupsResult;

/// Sole terminal observer for one submitted classic-group description batch.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeClassicGroups {
    inner: AdminDescribeClassicGroups,
}

impl DescribeClassicGroups {
    pub(crate) const fn from_bridge(inner: AdminDescribeClassicGroups) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DescribeClassicGroupsResult, KafkaError> {
        self.inner
            .wait()
            .map(DescribeClassicGroupsResult::from_consumer)
    }
}

impl Future for DescribeClassicGroups {
    type Output = Result<DescribeClassicGroupsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll(context) {
            Poll::Ready(Ok(result)) => {
                Poll::Ready(Ok(DescribeClassicGroupsResult::from_consumer(result)))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}
