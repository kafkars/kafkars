//! Named single-observer `StreamsGroup` description operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::describe_streams_group::AdminDescribeStreamsGroup};

use super::DescribeStreamsGroupResult;

/// Sole terminal observer for one submitted `StreamsGroup` description.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeStreamsGroup {
    inner: AdminDescribeStreamsGroup,
}

impl DescribeStreamsGroup {
    pub(crate) const fn from_bridge(inner: AdminDescribeStreamsGroup) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DescribeStreamsGroupResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeStreamsGroup {
    type Output = Result<DescribeStreamsGroupResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
