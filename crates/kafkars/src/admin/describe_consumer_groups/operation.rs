//! Named single-observer consumer-group description operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_describe_consumer_groups::AdminDescribeConsumerGroups};

use super::DescribeConsumerGroupsResult;

/// Sole terminal observer for one submitted group-description batch.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeConsumerGroups {
    inner: AdminDescribeConsumerGroups,
}

impl DescribeConsumerGroups {
    pub(crate) const fn from_bridge(inner: AdminDescribeConsumerGroups) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DescribeConsumerGroupsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeConsumerGroups {
    type Output = Result<DescribeConsumerGroupsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
