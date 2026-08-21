//! Named single-observer all-topic query for async and blocking Rust.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_topics_operation::AdminDescribeTopics};

use super::{BatchResult, TopicDescription};

/// Sole terminal observer for one submitted all-topic query.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListTopics {
    inner: AdminDescribeTopics,
}

impl ListTopics {
    pub(crate) const fn from_bridge(inner: AdminDescribeTopics) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<BatchResult<String, TopicDescription>, KafkaError> {
        self.inner.wait()
    }
}

impl Future for ListTopics {
    type Output = Result<BatchResult<String, TopicDescription>, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
