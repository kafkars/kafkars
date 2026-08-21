//! Named single-observer topic-ID `DescribeTopics` operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_topics_by_id_operation::AdminDescribeTopicsById};

use super::{BatchResult, TopicDescription};

/// Sole terminal observer for one submitted topic-ID batch.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeTopicsById {
    inner: AdminDescribeTopicsById,
}

impl DescribeTopicsById {
    pub(crate) const fn from_bridge(inner: AdminDescribeTopicsById) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<BatchResult<[u8; 16], TopicDescription>, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeTopicsById {
    type Output = Result<BatchResult<[u8; 16], TopicDescription>, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
