//! Named future for one caller-ordered, multi-group ShareGroup description.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::describe_share_groups::AdminDescribeShareGroups};

use super::DescribeShareGroupsResult;

/// A submitted multi-group ShareGroup description operation.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
#[derive(Debug)]
pub struct DescribeShareGroups {
    inner: AdminDescribeShareGroups,
}

impl DescribeShareGroups {
    pub(crate) const fn from_bridge(inner: AdminDescribeShareGroups) -> Self {
        Self { inner }
    }

    /// Blocks the current thread until the operation reaches a terminal result.
    pub fn wait(self) -> Result<DescribeShareGroupsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeShareGroups {
    type Output = Result<DescribeShareGroupsResult, KafkaError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(context)
    }
}
