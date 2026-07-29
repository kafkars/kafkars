//! Named future for one caller-ordered, multi-group StreamsGroup description.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::describe_streams_groups::AdminDescribeStreamsGroups};

use super::DescribeStreamsGroupsResult;

/// A submitted multi-group StreamsGroup description operation.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
#[derive(Debug)]
pub struct DescribeStreamsGroups {
    inner: AdminDescribeStreamsGroups,
}

impl DescribeStreamsGroups {
    pub(crate) const fn from_bridge(inner: AdminDescribeStreamsGroups) -> Self {
        Self { inner }
    }

    /// Blocks the current thread until the operation reaches a terminal result.
    pub fn wait(self) -> Result<DescribeStreamsGroupsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeStreamsGroups {
    type Output = Result<DescribeStreamsGroupsResult, KafkaError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(context)
    }
}
